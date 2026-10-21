import sql from 'mssql';
import { ConnectionConfig } from './types.js';

/** The slice of sql.ConnectionPool that getQueryPlan uses; lets tests supply a fake. */
export interface PlanPool {
  connect(): Promise<unknown>;
  close(): Promise<unknown>;
  request(): any;
}
export type PlanPoolFactory = (config: sql.config) => PlanPool;

/** Plan calls open their own connection each, so cap how many can be open at once. */
const MAX_CONCURRENT_PLANS = 3;
const PLAN_COLUMN_MARKER = 'XML Showplan';

export class SqlServerConnection {
  private pool: sql.ConnectionPool | null = null;
  private config: ConnectionConfig;
  private planSlotsInUse = 0;
  private planWaiters: Array<() => void> = [];

  constructor(
    config: ConnectionConfig,
    private readonly planPoolFactory: PlanPoolFactory = cfg => new sql.ConnectionPool(cfg)
  ) {
    this.config = config;
  }

  private buildSqlConfig(pool: sql.config['pool']): sql.config {
    return {
      server: this.config.server,
      database: this.config.database,
      user: this.config.user,
      password: this.config.password,
      port: this.config.port,
      options: {
        encrypt: this.config.encrypt,
        trustServerCertificate: this.config.trustServerCertificate,
        connectTimeout: this.config.connectionTimeout,
        requestTimeout: this.config.requestTimeout,
      },
      pool,
    };
  }

  async connect(): Promise<void> {
    if (this.pool) {
      return;
    }

    this.pool = new sql.ConnectionPool(
      this.buildSqlConfig({ max: 10, min: 0, idleTimeoutMillis: 30000 })
    );
    await this.pool.connect();
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.close();
      this.pool = null;
    }
  }

  /**
   * Builds the SESSION_CONTEXT preamble for a request.
   *
   * SESSION_CONTEXT is connection-scoped, and this class runs queries through a
   * pool, so the context has to be established in the same batch as the query
   * that depends on it -- setting it in a separate round trip would apply it to
   * whichever connection the pool happened to hand out at the time.
   *
   * Keys and values are bound as parameters rather than interpolated, so the
   * preamble carries no injection surface. It is built entirely from server
   * configuration; no part of it comes from the caller's query.
   *
   * When read-only keys are in use the EXEC is guarded on SESSION_CONTEXT()
   * being unset, because pooled connections are reused: re-running the EXEC
   * against an already-locked key raises error 15664.
   */
  private applySessionContext(request: sql.Request): string {
    const entries = Object.entries(this.config.sessionContext ?? {});
    if (entries.length === 0) {
      return '';
    }

    const readOnly = this.config.sessionContextReadOnly !== false;
    let preamble = '';

    entries.forEach(([key, value], i) => {
      const keyParam = `__sc_key_${i}`;
      const valueParam = `__sc_val_${i}`;

      request.input(keyParam, sql.NVarChar(128), key);
      if (typeof value === 'number') {
        request.input(valueParam, sql.Int, value);
      } else {
        // sql_variant, the type of sp_set_session_context's @value, cannot hold
        // the LOB types -- nvarchar(max) included -- so strings bind as
        // nvarchar(4000). parseSessionContext rejects anything longer.
        request.input(valueParam, sql.NVarChar(4000), value);
      }

      const exec =
        `EXEC sys.sp_set_session_context @key = @${keyParam}, ` +
        `@value = @${valueParam}` +
        (readOnly ? ', @read_only = 1' : '') +
        ';';

      preamble += readOnly
        ? `IF SESSION_CONTEXT(@${keyParam}) IS NULL ${exec}\n`
        : `${exec}\n`;
    });

    return preamble;
  }

  async query<T = any>(queryText: string): Promise<sql.IResult<T>> {
    if (!this.pool) {
      throw new Error('Database connection not established');
    }

    const request = this.pool.request();
    const preamble = this.applySessionContext(request);
    return await request.query(preamble + queryText);
  }

  /**
   * Tears down and reopens the pool, clearing SESSION_CONTEXT on every
   * connection.
   *
   * Read-only keys cannot be cleared from T-SQL -- they are released only when
   * the connection resets. tedious can reset an individual connection
   * (Connection.reset), but mssql neither calls it nor exposes a per-checkout
   * hook, so recycling the pool is the only route through the public API.
   * Callers need this to change a session context value that was set read-only.
   */
  async resetSessionContext(
    sessionContext?: ConnectionConfig['sessionContext']
  ): Promise<void> {
    if (arguments.length > 0) {
      this.config = { ...this.config, sessionContext };
    }

    await this.disconnect();
    await this.connect();
  }

  /**
   * Returns the execution plan XML for a query, on a connection used for this
   * one call and then closed.
   *
   * Plan modes are session settings (SET SHOWPLAN_XML, SET STATISTICS XML). On a
   * pooled connection a failure between the SET and its OFF -- a timeout, or a
   * permission error, which aborts the batch -- leaves the connection in plan
   * mode for whoever uses it next, and in SHOWPLAN_XML mode queries return plans
   * instead of rows. So this never touches the shared pool: the connection is
   * closed in every case, which ends the session and takes the setting with it.
   * Closing also cancels a query still running server-side.
   *
   * The pool created here has a single connection, so the SET and the query
   * that follows it are guaranteed to share a session.
   *
   * estimated=true compiles the query without running it (SET SHOWPLAN_XML must
   * be alone in its batch, hence separate batches). estimated=false runs it with
   * SET STATISTICS XML ON in one batch; result rows are discarded as they
   * arrive so a large result set is never held in memory. The SETs go through
   * batch(): mssql's query() sends text via sp_executesql, where they do not
   * persist.
   *
   * The caller must have validated queryText; it is embedded as given, and no
   * row limit is added, so the plan is for the query as written.
   */
  async getQueryPlan(queryText: string, estimated: boolean): Promise<string> {
    await this.acquirePlanSlot();
    const pool = this.planPoolFactory(this.buildSqlConfig({ max: 1, min: 0 }));
    try {
      await pool.connect();

      // Row-level security needs the session context before the query is
      // compiled. Fresh connection, so there is no already-set key to guard.
      const contextRequest = pool.request();
      const preamble = this.applySessionContext(contextRequest);
      if (preamble) {
        await contextRequest.query(preamble);
      }

      if (estimated) {
        await pool.request().batch('SET SHOWPLAN_XML ON');
        const result = await pool.request().batch(queryText);
        return this.extractPlan(result.recordsets);
      }

      return await this.runCapturingPlan(
        pool,
        `SET STATISTICS XML ON;\n${queryText}\n;\nSET STATISTICS XML OFF;`
      );
    } finally {
      try {
        await pool.close();
      } catch {
        // The connection is discarded either way.
      }
      this.releasePlanSlot();
    }
  }

  private extractPlan(recordsets: ReadonlyArray<ReadonlyArray<Record<string, unknown>>>): string {
    const plans: string[] = [];
    for (const recordset of recordsets ?? []) {
      for (const row of recordset) {
        const value = Object.values(row)[0];
        if (typeof value === 'string' && value.startsWith('<ShowPlanXML')) {
          plans.push(value);
        }
      }
    }
    if (plans.length === 0) {
      throw new Error('SQL Server returned no execution plan');
    }
    return plans.join('\n');
  }

  private runCapturingPlan(pool: PlanPool, batchText: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const request = pool.request();
      request.stream = true;
      let inPlanRecordset = false;
      const plans: string[] = [];

      request.on('recordset', (columns: Record<string, unknown>) => {
        inPlanRecordset = Object.keys(columns).some(name => name.includes(PLAN_COLUMN_MARKER));
      });
      request.on('row', (row: Record<string, unknown>) => {
        if (!inPlanRecordset) {
          return; // a result row: discarded, never accumulated
        }
        const value = Object.values(row)[0];
        if (typeof value === 'string') {
          plans.push(value);
        }
      });
      request.on('error', reject);
      request.on('done', () =>
        plans.length > 0
          ? resolve(plans.join('\n'))
          : reject(new Error('SQL Server returned no execution plan'))
      );

      request.batch(batchText);
    });
  }

  private async acquirePlanSlot(): Promise<void> {
    if (this.planSlotsInUse < MAX_CONCURRENT_PLANS) {
      this.planSlotsInUse++;
      return;
    }
    await new Promise<void>(resolve => this.planWaiters.push(resolve));
  }

  private releasePlanSlot(): void {
    const next = this.planWaiters.shift();
    if (next) {
      next(); // hand the slot straight to the next waiter
    } else {
      this.planSlotsInUse--;
    }
  }

  async testConnection(): Promise<boolean> {
    try {
      await this.connect();
      const result = await this.query('SELECT 1 as test');
      return result.recordset.length > 0;
    } catch (error) {
      return false;
    }
  }

  isConnected(): boolean {
    return this.pool !== null && this.pool.connected;
  }

  getConfig(): Readonly<ConnectionConfig> {
    return { ...this.config };
  }
}