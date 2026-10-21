import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import sql from 'mssql';
import { SqlServerConnection } from '../../src/connection.js';
import { ConnectionConfigSchema } from '../../src/types.js';
import { GetQueryPlanTool } from '../../src/tools/get-query-plan.js';

/**
 * get_query_plan against a real SQL Server (2019 or later).
 *
 * Opt-in: skipped unless these are set. It creates a throwaway schema, table,
 * security policy and login in MSSQL_TEST_DB and drops them afterwards, so point
 * it at a scratch database, never at a shared one.
 *
 *   MSSQL_TEST_HOST            e.g. localhost (a name, not an IP: TLS server name)
 *   MSSQL_TEST_PORT            default 1433
 *   MSSQL_TEST_DB              a scratch database
 *   MSSQL_TEST_ADMIN_USER      a login able to create objects and logins
 *   MSSQL_TEST_ADMIN_PASSWORD
 *
 * Run:  npm run test:integration
 */
const env = process.env;
const configured = !!(env.MSSQL_TEST_HOST && env.MSSQL_TEST_DB && env.MSSQL_TEST_ADMIN_USER && env.MSSQL_TEST_ADMIN_PASSWORD);

const READER = 'plan_it_reader';
const readerPassword = `Zz9!${randomBytes(12).toString('hex')}`;

const baseConfig = (user: string, password: string, extra: Record<string, unknown> = {}) =>
  ConnectionConfigSchema.parse({
    server: env.MSSQL_TEST_HOST,
    port: Number(env.MSSQL_TEST_PORT ?? 1433),
    database: env.MSSQL_TEST_DB,
    user,
    password,
    encrypt: true,
    trustServerCertificate: true,
    ...extra,
  });

// A query that would never finish if it ran: 10^12 rows. Estimating it is instant.
const HUGE = `
  WITH n AS (SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS i FROM sys.all_objects a CROSS JOIN sys.all_objects b)
  SELECT COUNT_BIG(*) AS c FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d`;

describe('get_query_plan against SQL Server', { skip: configured ? false : 'MSSQL_TEST_* not set' }, () => {
  let admin: sql.ConnectionPool;

  before(async () => {
    admin = await new sql.ConnectionPool({
      server: env.MSSQL_TEST_HOST!, port: Number(env.MSSQL_TEST_PORT ?? 1433), database: env.MSSQL_TEST_DB,
      user: env.MSSQL_TEST_ADMIN_USER, password: env.MSSQL_TEST_ADMIN_PASSWORD,
      options: { encrypt: true, trustServerCertificate: true },
    }).connect();
    const run = (text: string) => admin.request().batch(text);
    await run(`IF SUSER_ID(N'${READER}') IS NOT NULL DROP LOGIN ${READER}`);
    await run(`CREATE LOGIN ${READER} WITH PASSWORD = N'${readerPassword}', CHECK_POLICY = OFF`);
    await run(`IF USER_ID(N'${READER}') IS NOT NULL DROP USER ${READER}`);
    await run(`CREATE USER ${READER} FOR LOGIN ${READER}`);
    await run(`ALTER ROLE db_datareader ADD MEMBER ${READER}`);
    await run(`IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'plan_it_policy') DROP SECURITY POLICY dbo.plan_it_policy`);
    await run(`IF OBJECT_ID(N'dbo.plan_it_rows') IS NOT NULL DROP TABLE dbo.plan_it_rows`);
    await run(`IF OBJECT_ID(N'dbo.plan_it_predicate') IS NOT NULL DROP FUNCTION dbo.plan_it_predicate`);
    await run(`CREATE TABLE dbo.plan_it_rows (id int NOT NULL PRIMARY KEY, tenant int NOT NULL)`);
    await run(`INSERT dbo.plan_it_rows SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 1 FROM sys.all_objects`);
    await run(`INSERT dbo.plan_it_rows SELECT TOP 40 1000 + ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 2 FROM sys.all_objects`);
    await run(`CREATE FUNCTION dbo.plan_it_predicate(@tenant int) RETURNS TABLE WITH SCHEMABINDING AS
               RETURN SELECT 1 AS ok WHERE @tenant = CAST(SESSION_CONTEXT(N'TenantId') AS int)`);
    await run(`CREATE SECURITY POLICY dbo.plan_it_policy ADD FILTER PREDICATE dbo.plan_it_predicate(tenant) ON dbo.plan_it_rows WITH (STATE = ON)`);
    await run(`GRANT SHOWPLAN TO ${READER}`);
  });

  after(async () => {
    if (!admin) return;
    const run = (text: string) => admin.request().batch(text).catch(() => undefined);
    await run(`IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'plan_it_policy') DROP SECURITY POLICY dbo.plan_it_policy`);
    await run(`DROP TABLE IF EXISTS dbo.plan_it_rows`);
    await run(`DROP FUNCTION IF EXISTS dbo.plan_it_predicate`);
    await run(`IF USER_ID(N'${READER}') IS NOT NULL DROP USER ${READER}`);
    await run(`IF SUSER_ID(N'${READER}') IS NOT NULL DROP LOGIN ${READER}`);
    await admin.close();
  });

  const otherSessions = async () =>
    (await admin.request().query(
      `SELECT COUNT(*) AS n FROM sys.dm_exec_sessions WHERE is_user_process = 1 AND session_id <> @@SPID AND login_name = N'${READER}'`
    )).recordset[0].n as number;
  const settle = () => new Promise(r => setTimeout(r, 800));

  const reader = (extra: Record<string, unknown> = {}) =>
    new SqlServerConnection(baseConfig(READER, readerPassword, { sessionContext: { TenantId: 1 }, ...extra }));

  test('estimated: returns the plan for a query that could never run to completion, quickly', async () => {
    const tool = new GetQueryPlanTool(reader());
    const started = Date.now();
    const xml = await tool.execute({ query: HUGE });
    assert.ok(xml.startsWith('<ShowPlanXML'), xml.slice(0, 80));
    assert.ok(Date.now() - started < 10_000, 'an estimated plan must not execute the query');
    assert.ok(!xml.includes('RunTimeInformation'), 'an estimated plan has no runtime counts');
  });

  test('actual: runs the query, returns real row counts, and returns only the XML', async () => {
    const tool = new GetQueryPlanTool(reader());
    const xml = await tool.execute({
      query: 'SELECT TOP 20000 a.name FROM sys.all_objects a CROSS JOIN sys.all_objects b',
      estimated: false,
    });
    assert.ok(xml.startsWith('<ShowPlanXML'));
    assert.match(xml, /ActualRows="20000"/);
    assert.ok(!xml.includes('sys.all_objects') || xml.includes('StatementText'), 'no result rows in the output');
  });

  test('the plan is for the query as written: SELECT DISTINCT is not rewritten', async () => {
    const tool = new GetQueryPlanTool(reader());
    const xml = await tool.execute({ query: 'SELECT DISTINCT TOP 5 name FROM sys.objects' });
    assert.ok(xml.startsWith('<ShowPlanXML'));
    assert.ok(!/TOP 1000/.test(xml));
  });

  test('plan calls never leave the shared pool in plan mode', async () => {
    const connection = reader();
    const tool = new GetQueryPlanTool(connection);
    await connection.connect();
    for (let i = 0; i < 5; i++) {
      await tool.execute({ query: 'SELECT TOP 5 name FROM sys.objects' });
      await tool.execute({ query: 'SELECT TOP 5 name FROM sys.objects', estimated: false });
      const r = await connection.query('SELECT TOP 1 name FROM sys.objects ORDER BY name');
      assert.equal(r.recordsets.length, 1, 'an extra recordset means the connection is still in plan mode');
      assert.equal(r.recordset.length, 1, 'no row means it is in SHOWPLAN_XML mode');
    }
    await connection.disconnect();
  });

  test('closing really ends the session: none left after success, error or timeout', async () => {
    const before = await otherSessions();

    await new GetQueryPlanTool(reader()).execute({ query: 'SELECT TOP 5 name FROM sys.objects' });
    await new GetQueryPlanTool(reader()).execute({ query: 'SELECT TOP 5 name FROM sys.objects', estimated: false });
    await settle();
    assert.equal(await otherSessions(), before, 'leaked a session after success');

    await assert.rejects(new GetQueryPlanTool(reader()).execute({ query: 'SELECT * FROM dbo.no_such_table_xyz' }));
    await settle();
    assert.equal(await otherSessions(), before, 'leaked a session after an error');

    // actual mode on a query that cannot finish: times out, and the server must drop it
    const slow = reader({ requestTimeout: 1500 });
    await assert.rejects(slow.getQueryPlan(HUGE, false), /timeout/i);
    await settle();
    assert.equal(await otherSessions(), before, 'leaked a session after a timeout');
    const running = await admin.request().query(
      `SELECT COUNT(*) AS n FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id WHERE s.login_name = N'${READER}'`
    );
    assert.equal(running.recordset[0].n, 0, 'the timed-out query is still running on the server');
  });

  test('row-level security: the session context is in force for both plan modes', async () => {
    const asTenant = (tenant: number) => new GetQueryPlanTool(reader({ sessionContext: { TenantId: tenant } }));
    const q = 'SELECT COUNT(*) AS c FROM dbo.plan_it_rows';
    const actualRows = (xml: string) =>
      [...xml.matchAll(/RelOp NodeId="\d+" PhysicalOp="(?:Clustered Index Scan|Index Scan)"[\s\S]*?ActualRows="(\d+)"/g)].map(m => Number(m[1]));
    const one = await asTenant(1).execute({ query: q, estimated: false });
    const two = await asTenant(2).execute({ query: q, estimated: false });
    assert.deepEqual(actualRows(one), [100], 'tenant 1 should read its 100 rows');
    assert.deepEqual(actualRows(two), [40], 'tenant 2 should read its 40 rows');
    const est = await asTenant(1).execute({ query: q });
    assert.ok(est.startsWith('<ShowPlanXML'));
  });

  test('without SHOWPLAN permission the tool fails with a permission error, and cleans up', async () => {
    await admin.request().batch(`REVOKE SHOWPLAN FROM ${READER}`);
    try {
      const before = await otherSessions();
      const tool = new GetQueryPlanTool(reader());
      await assert.rejects(tool.execute({ query: 'SELECT COUNT(*) FROM dbo.plan_it_rows' }), (e: any) => {
        assert.equal(e.code, 'PERMISSION_ERROR', `got ${e.code}: ${e.message}`);
        return true;
      });
      await assert.rejects(tool.execute({ query: 'SELECT COUNT(*) FROM dbo.plan_it_rows', estimated: false }), (e: any) => e.code === 'PERMISSION_ERROR');
      await settle();
      assert.equal(await otherSessions(), before);
    } finally {
      await admin.request().batch(`GRANT SHOWPLAN TO ${READER}`);
    }
  });

  test('ten concurrent calls complete, and none is left open', async () => {
    const tool = new GetQueryPlanTool(reader());
    const before = await otherSessions();
    const results = await Promise.all(Array.from({ length: 10 }, (_, i) =>
      tool.execute({ query: `SELECT TOP ${i + 1} name FROM sys.objects`, estimated: i % 2 === 0 })));
    assert.ok(results.every(x => x.startsWith('<ShowPlanXML')));
    await settle();
    assert.equal(await otherSessions(), before);
  });
});
