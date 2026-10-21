import { z } from 'zod';
import { BaseTool } from './base.js';
import { QueryValidator } from '../security.js';
import { ErrorHandler } from '../errors.js';

export class GetQueryPlanTool extends BaseTool {
  getName(): string {
    return 'get_query_plan';
  }

  getDescription(): string {
    return (
      'Get the SQL Server execution plan, as XML, for a read-only SELECT. ' +
      'With estimated=true (the default) the query is compiled but NOT run, so it is safe on large tables. ' +
      'With estimated=false the query IS run, its result rows are discarded, and the actual plan is returned ' +
      'with real row counts and timings. The plan is for the query exactly as written: no row limit is added, ' +
      'unlike execute_query, which adds TOP.'
    );
  }

  getInputSchema(): any {
    return {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'SQL SELECT query to get the plan for (read-only; a single statement)',
        },
        estimated: {
          type: 'boolean',
          description:
            'true (default): estimated plan, query is not executed. false: actual plan, query is executed.',
        },
      },
      required: ['query'],
    };
  }

  async execute(params: { query?: string; estimated?: boolean }): Promise<string> {
    const { query, estimated } = z
      .object({
        query: z
          .string()
          .min(1, 'Query cannot be empty')
          .max(10000, 'Query cannot exceed 10,000 characters')
          .refine(q => q.trim().length > 0, 'Query cannot be only whitespace'),
        estimated: z.boolean().optional().default(true),
      })
      .parse(params);

    const validation = QueryValidator.validateQuery(query);
    if (!validation.isValid) {
      throw new Error(`Query validation failed: ${validation.error}`);
    }

    try {
      return await this.connection.getQueryPlan(QueryValidator.sanitizeQuery(query), estimated);
    } catch (error) {
      throw ErrorHandler.handleSqlServerError(error);
    }
  }
}
