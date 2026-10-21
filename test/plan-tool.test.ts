import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { SqlServerConnection, PlanPool } from '../src/connection.js';
import { ConnectionConfigSchema } from '../src/types.js';
import { GetQueryPlanTool } from '../src/tools/get-query-plan.js';

/**
 * get_query_plan, tested without a database.
 *
 * The fake pool records every call so the tests can assert on exactly what the
 * server would send, and in what order. Behaviour against a real SQL Server
 * (what each batch returns, that closing really ends the session) is covered by
 * test/integration/plan.integration.test.ts, which needs a database.
 */

const PLAN_COLUMN = 'Microsoft SQL Server 2005 XML Showplan';
const PLAN_XML = '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><Batch/></ShowPlanXML>';

interface Scenario {
  failConnect?: boolean;
  failBatchContaining?: string;
  streamError?: boolean;
  noPlan?: boolean;
  resultRows?: number;
  delayMs?: number;
}

function harness(scenario: Scenario = {}, extraConfig: Record<string, unknown> = {}) {
  const calls: string[] = [];
  const bound: Array<{ name: string; value: unknown }> = [];
  const pools: Array<{ max: number | undefined }> = [];
  let open = 0;
  let maxOpen = 0;
  let rowsEmitted = 0;

  const makePool = (cfg: any): PlanPool => {
    pools.push({ max: cfg.pool?.max });
    return {
      async connect() {
        if (scenario.delayMs) await new Promise(r => setTimeout(r, scenario.delayMs));
        if (scenario.failConnect) throw new Error('connect failed');
        open++;
        maxOpen = Math.max(maxOpen, open);
        calls.push('connect');
      },
      async close() {
        open--;
        calls.push('close');
      },
      request() {
        const req: any = new EventEmitter();
        req.stream = false;
        req.input = (name: string, _type: unknown, value: unknown) => bound.push({ name, value });
        req.query = async (text: string) => {
          calls.push(`query:${text}`);
          return { recordsets: [] };
        };
        req.batch = (text: string) => {
          calls.push(`batch:${text}`);
          if (scenario.failBatchContaining && text.includes(scenario.failBatchContaining)) {
            if (req.stream) {
              setImmediate(() => req.emit('error', new Error('batch failed')));
              return;
            }
            return Promise.reject(new Error('batch failed'));
          }
          const planRecordset = scenario.noPlan ? [] : [{ [PLAN_COLUMN]: PLAN_XML }];
          if (!req.stream) {
            // hold the connection for a moment so concurrent calls genuinely overlap
            return new Promise(resolve =>
              setTimeout(() => resolve({ recordsets: [planRecordset] }), scenario.delayMs ?? 0)
            );
          }
          // streaming (actual plan): result rows first, then the plan
          setImmediate(async () => {
            if (scenario.streamError) return req.emit('error', new Error('stream failed'));
            const n = scenario.resultRows ?? 3;
            req.emit('recordset', { obj: {}, col: {} });
            for (let i = 0; i < n; i++) {
              rowsEmitted++;
              req.emit('row', { obj: `row${i}`, col: i });
            }
            if (!scenario.noPlan) {
              req.emit('recordset', { [PLAN_COLUMN]: {} });
              req.emit('row', { [PLAN_COLUMN]: PLAN_XML });
            }
            req.emit('done');
          });
        };
        return req;
      },
    };
  };

  const config = ConnectionConfigSchema.parse({ server: 's', user: 'u', password: 'p', ...extraConfig });
  const connection = new SqlServerConnection(config, makePool);
  const tool = new GetQueryPlanTool(connection);
  return { connection, tool, calls, bound, pools, stats: () => ({ open, maxOpen, rowsEmitted }) };
}

const Q = 'SELECT id, name FROM dbo.things WHERE id > 5';

// ---------------------------------------------------------------------------

test('estimated (the default): SET SHOWPLAN_XML ON, the query, in separate batches', async () => {
  const h = harness();
  const xml = await h.tool.execute({ query: Q });
  assert.equal(xml, PLAN_XML);
  assert.deepEqual(h.calls, ['connect', 'batch:SET SHOWPLAN_XML ON', `batch:${Q}`, 'close']);
});

test('estimated: the query is sent exactly as written, with no TOP added', async () => {
  const h = harness();
  await h.tool.execute({ query: 'SELECT DISTINCT x FROM t', estimated: true });
  const sent = h.calls.filter(c => c.startsWith('batch:'));
  assert.equal(sent[1], 'batch:SELECT DISTINCT x FROM t');
  assert.ok(!h.calls.join('\n').includes('TOP '), 'a row limit would change the plan');
});

test('actual: one batch with SET STATISTICS XML ON ... OFF, and only the XML comes back', async () => {
  const h = harness({ resultRows: 5 });
  const xml = await h.tool.execute({ query: Q, estimated: false });
  assert.equal(xml, PLAN_XML, 'result rows must not be mixed into the output');
  assert.deepEqual(h.calls, [
    'connect',
    `batch:SET STATISTICS XML ON;\n${Q}\n;\nSET STATISTICS XML OFF;`,
    'close',
  ]);
});

test('actual: result rows are discarded as they arrive, never accumulated', async () => {
  const h = harness({ resultRows: 200_000 });
  const xml = await h.tool.execute({ query: Q, estimated: false });
  assert.equal(xml, PLAN_XML);
  assert.equal(h.stats().rowsEmitted, 200_000);
});

test('actual: a trailing line comment cannot swallow the closing OFF', async () => {
  const h = harness();
  await h.tool.execute({ query: 'SELECT 1 -- note', estimated: false });
  const batch = h.calls.find(c => c.startsWith('batch:'))!;
  assert.ok(batch.includes('-- note\n;\nSET STATISTICS XML OFF;'), batch);
});

test('a CTE can follow the SET: it is preceded by a terminated statement', async () => {
  const h = harness();
  await h.tool.execute({ query: 'WITH c AS (SELECT 1 AS n) SELECT n FROM c', estimated: false });
  const batch = h.calls.find(c => c.startsWith('batch:'))!;
  assert.ok(batch.startsWith('batch:SET STATISTICS XML ON;\nWITH c AS'), batch);
});

// ---------------------------------------------------------------------------
// The connection is never shared and never left open.
// ---------------------------------------------------------------------------

test('every call gets its own single-connection pool, closed afterwards', async () => {
  const h = harness();
  await h.tool.execute({ query: Q });
  await h.tool.execute({ query: Q, estimated: false });
  assert.deepEqual(h.pools, [{ max: 1 }, { max: 1 }]);
  assert.equal(h.calls.filter(c => c === 'connect').length, 2);
  assert.equal(h.calls.filter(c => c === 'close').length, 2);
  assert.equal(h.stats().open, 0);
});

test('the shared pool is never opened by a plan call', async () => {
  const h = harness();
  await h.tool.execute({ query: Q });
  assert.equal(h.connection.isConnected(), false);
});

test('closed when the estimated batch fails', async () => {
  const h = harness({ failBatchContaining: Q });
  await assert.rejects(h.tool.execute({ query: Q }));
  assert.equal(h.calls.at(-1), 'close');
  assert.equal(h.stats().open, 0);
});

test('closed when the actual-plan stream errors', async () => {
  const h = harness({ streamError: true });
  await assert.rejects(h.tool.execute({ query: Q, estimated: false }));
  assert.equal(h.calls.at(-1), 'close');
  assert.equal(h.stats().open, 0);
});

test('a failed connect still releases its concurrency slot', async () => {
  const h = harness({ failConnect: true });
  for (let i = 0; i < 6; i++) await assert.rejects(h.tool.execute({ query: Q }));
  // six failures with a cap of three would hang here if slots leaked
  const ok = harness();
  await ok.tool.execute({ query: Q });
});

test('no plan returned is an error, not an empty success', async () => {
  const est = harness({ noPlan: true });
  await assert.rejects(est.tool.execute({ query: Q }), /no execution plan/i);
  const act = harness({ noPlan: true });
  await assert.rejects(act.tool.execute({ query: Q, estimated: false }), /no execution plan/i);
});

test('concurrent plan calls are capped at three open connections', async () => {
  const h = harness({ delayMs: 15 });
  await Promise.all(Array.from({ length: 10 }, () => h.tool.execute({ query: Q })));
  assert.equal(h.stats().maxOpen <= 3, true, `maxOpen was ${h.stats().maxOpen}`);
  assert.equal(h.stats().maxOpen, 3, 'the cap should be reached, or the test proves nothing');
  assert.equal(h.stats().open, 0);
  assert.equal(h.calls.filter(c => c === 'close').length, 10);
});

// ---------------------------------------------------------------------------
// Validation happens before any connection is opened.
// ---------------------------------------------------------------------------

test('queries the read-only guard rejects never open a connection', async () => {
  const h = harness();
  for (const bad of [
    'DELETE FROM t',
    'SELECT 1; SELECT 2',
    'SELECT * INTO copy FROM t',
    'SELECT 1 SHUTDOWN',
    "SELECT 'unterminated",
    'EXEC sp_who',
  ]) {
    await assert.rejects(h.tool.execute({ query: bad }), /validation failed/i, bad);
  }
  assert.deepEqual(h.calls, [], 'nothing may be sent for a rejected query');
});

test('input is validated: empty, missing and wrongly typed arguments', async () => {
  const h = harness();
  await assert.rejects(h.tool.execute({ query: '' }));
  await assert.rejects(h.tool.execute({ query: '   ' }));
  await assert.rejects(h.tool.execute({} as any));
  await assert.rejects(h.tool.execute({ query: Q, estimated: 'yes' as any }));
  await assert.rejects(h.tool.execute({ query: 'SELECT ' + 'x'.repeat(10_001) }));
  assert.deepEqual(h.calls, []);
});

test('a name that merely contains a keyword is allowed through', async () => {
  const h = harness();
  const xml = await h.tool.execute({ query: 'SELECT * FROM order_execution WHERE deleted_at IS NULL' });
  assert.equal(xml, PLAN_XML);
});

// ---------------------------------------------------------------------------
// Row-level security
// ---------------------------------------------------------------------------

test('session context is set first, as its own step, before any plan mode', async () => {
  const h = harness({}, { sessionContext: { TenantId: 42 } });
  await h.tool.execute({ query: Q });
  const order = h.calls.map(c => c.split(':')[0] + (c.startsWith('batch:SET') ? ':SET' : ''));
  assert.deepEqual(order, ['connect', 'query', 'batch:SET', 'batch', 'close']);
  const context = h.calls[1];
  assert.ok(context.includes('sp_set_session_context'), context);
  assert.ok(!context.includes('42') && !context.includes('TenantId'), 'values must be bound, not interpolated');
  assert.deepEqual(
    h.bound.map(b => b.value),
    ['TenantId', 42]
  );
});

test('session context is applied in actual mode too', async () => {
  const h = harness({}, { sessionContext: { TenantId: 42 } });
  await h.tool.execute({ query: Q, estimated: false });
  assert.ok(h.calls[1].startsWith('query:') && h.calls[1].includes('sp_set_session_context'));
  assert.ok(h.calls[2].startsWith('batch:SET STATISTICS XML ON'));
});

test('without a configured session context nothing extra is sent', async () => {
  const h = harness();
  await h.tool.execute({ query: Q });
  assert.ok(!h.calls.some(c => c.includes('sp_set_session_context')));
});

// ---------------------------------------------------------------------------
// The tool itself
// ---------------------------------------------------------------------------

test('the tool describes itself accurately', () => {
  const tool = new GetQueryPlanTool({} as any);
  assert.equal(tool.getName(), 'get_query_plan');
  const schema = tool.getInputSchema();
  assert.deepEqual(schema.required, ['query']);
  assert.equal(schema.properties.estimated.type, 'boolean');
  const description = tool.getDescription();
  assert.match(description, /NOT run/);
  assert.match(description, /estimated=false/);
  assert.match(description, /no row limit is added/);
});
