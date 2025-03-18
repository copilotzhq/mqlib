/**
 * PostgreSQL adapter for MQLib
 * Implements the SqlAdapter interface for PostgreSQL
 */

import { SqlAdapter, SqlQuery } from "../../interfaces/adapter.ts";
import { SqlDialect } from "../../types/sql.ts";
import { UpdateOperator } from "../../types/update.ts";
import { Filter } from "../../types/filter.ts";
import { Document } from "../../types/document.ts";
import { BaseSqlAdapter } from "../base_adapter.ts";

/**
 * PostgreSQL adapter implementation
 */
export class PostgresAdapter extends BaseSqlAdapter {
  /**
   * The SQL dialect
   */
  readonly dialect = SqlDialect.POSTGRES;

  /**
   * Escapes an identifier (table name, column name) for PostgreSQL
   * @param identifier The identifier to escape
   * @returns The escaped identifier
   */
  override escapeIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Gets the parameter placeholder for PostgreSQL ($1, $2, etc.)
   * @param index The parameter index (0-based)
   * @returns The parameter placeholder
   */
  override getParameterPlaceholder(index: number): string {
    return `$${index + 1}`;
  }

  /**
   * Translates a JSON schema to a PostgreSQL CREATE TABLE statement
   * @param tableName The name of the table to create
   * @param schema The JSON schema
   * @returns The SQL query
   */
  override translateSchema(tableName: string, schema: object): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const properties = (schema as any).properties || {};
    const required = (schema as any).required || [];
    
    const columns: string[] = [];
    
    for (const [key, value] of Object.entries(properties)) {
      const propDef = value as any;
      let sqlType = "TEXT";
      let constraints = "";
      
      if (key === "_id") {
        constraints = " PRIMARY KEY";
      }
      
      if (required.includes(key)) {
        constraints += " NOT NULL";
      }
      
      switch (propDef.type) {
        case "string":
          sqlType = "TEXT";
          break;
        case "integer":
          sqlType = "INTEGER";
          break;
        case "number":
          sqlType = "NUMERIC";
          break;
        case "boolean":
          sqlType = "BOOLEAN";
          break;
        case "object":
          sqlType = "JSONB";
          break;
        case "array":
          sqlType = "JSONB";
          break;
      }
      
      columns.push(`${this.escapeIdentifier(key)} ${sqlType}${constraints}`);
    }
    
    const sql = `CREATE TABLE IF NOT EXISTS ${escapedTableName} (
      ${columns.join(",\n      ")}
    )`;
    
    return { sql, params: [] };
  }

  /**
   * Translates a create index operation to a PostgreSQL CREATE INDEX statement
   * @param tableName The name of the table
   * @param indexName The name of the index
   * @param fields The fields to index
   * @param options Index options
   * @returns The SQL query
   */
  override translateCreateIndex(
    tableName: string, 
    indexName: string, 
    fields: Record<string, 1 | -1>, 
    options?: { unique?: boolean; sparse?: boolean }
  ): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const escapedIndexName = this.escapeIdentifier(`idx_${tableName}_${indexName}`);
    const unique = options?.unique ? "UNIQUE " : "";
    
    const fieldEntries = Object.entries(fields);
    const indexFields = fieldEntries.map(([field, direction]) => {
      const escapedField = this.escapeIdentifier(field);
      return `${escapedField} ${direction === 1 ? "ASC" : "DESC"}`;
    });
    
    const sql = `CREATE ${unique}INDEX IF NOT EXISTS ${escapedIndexName} ON ${escapedTableName} (${indexFields.join(", ")})`;
    
    return { sql, params: [] };
  }

  /**
   * Builds SQL conditions for MongoDB-style operators
   * @param column The column name
   * @param operators The operators
   * @param params The parameters array to append to
   * @returns The SQL condition string
   */
  protected buildOperatorConditions(column: string, operators: Record<string, unknown>, params: unknown[]): string {
    const conditions: string[] = [];
    const escapedColumn = this.escapeIdentifier(column);
    
    for (const [op, value] of Object.entries(operators)) {
      switch (op) {
        case "$eq":
          conditions.push(`${escapedColumn} = ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$ne":
          conditions.push(`${escapedColumn} != ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$gt":
          conditions.push(`${escapedColumn} > ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$gte":
          conditions.push(`${escapedColumn} >= ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$lt":
          conditions.push(`${escapedColumn} < ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$lte":
          conditions.push(`${escapedColumn} <= ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$in":
          if (Array.isArray(value) && value.length > 0) {
            const placeholders = value.map((_, i) => this.getParameterPlaceholder(params.length + i));
            conditions.push(`${escapedColumn} IN (${placeholders.join(", ")})`);
            params.push(...value);
          } else {
            conditions.push("FALSE"); // Empty $in array should never match
          }
          break;
        case "$nin":
          if (Array.isArray(value) && value.length > 0) {
            const placeholders = value.map((_, i) => this.getParameterPlaceholder(params.length + i));
            conditions.push(`${escapedColumn} NOT IN (${placeholders.join(", ")})`);
            params.push(...value);
          } else {
            conditions.push("TRUE"); // Empty $nin array should match everything
          }
          break;
        case "$exists":
          if (value) {
            conditions.push(`${escapedColumn} IS NOT NULL`);
          } else {
            conditions.push(`${escapedColumn} IS NULL`);
          }
          break;
        case "$regex":
          conditions.push(`${escapedColumn} ~ ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$like":
          conditions.push(`${escapedColumn} LIKE ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$ilike":
          conditions.push(`${escapedColumn} ILIKE ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$contains":
          // For JSONB arrays
          conditions.push(`${escapedColumn} @> ${this.getParameterPlaceholder(params.length)}::jsonb`);
          params.push(JSON.stringify([value]));
          break;
        case "$containsAny":
          // For JSONB arrays
          if (Array.isArray(value)) {
            conditions.push(`${escapedColumn} ?| ${this.getParameterPlaceholder(params.length)}`);
            params.push(value);
          }
          break;
      }
    }
    
    return conditions.join(" AND ");
  }

  /**
   * Builds a SET clause for UPDATE statements
   * @param update The update operator
   * @param params The parameters array to append to
   * @returns The SET clause
   */
  override buildSetClause<T>(update: UpdateOperator<T>, params: unknown[]): string {
    const setClauses: string[] = [];
    
    // Handle $set operator
    if (update.$set) {
      for (const [field, value] of Object.entries(update.$set)) {
        const escapedField = this.escapeIdentifier(field);
        setClauses.push(`${escapedField} = ${this.getParameterPlaceholder(params.length)}`);
        params.push(value);
      }
    }
    
    // Handle $inc operator
    if (update.$inc) {
      for (const [field, value] of Object.entries(update.$inc)) {
        const escapedField = this.escapeIdentifier(field);
        setClauses.push(`${escapedField} = ${escapedField} + ${this.getParameterPlaceholder(params.length)}`);
        params.push(value);
      }
    }
    
    // Handle $unset operator (set fields to NULL)
    if (update.$unset) {
      for (const field of Object.keys(update.$unset)) {
        const escapedField = this.escapeIdentifier(field);
        setClauses.push(`${escapedField} = NULL`);
      }
    }
    
    // Handle $push operator (for JSONB arrays)
    if (update.$push) {
      for (const [field, value] of Object.entries(update.$push)) {
        const escapedField = this.escapeIdentifier(field);
        setClauses.push(`${escapedField} = COALESCE(${escapedField}, '[]'::jsonb) || ${this.getParameterPlaceholder(params.length)}::jsonb`);
        params.push(JSON.stringify([value]));
      }
    }
    
    // Handle $pull operator (for JSONB arrays)
    if (update.$pull) {
      for (const [field, value] of Object.entries(update.$pull)) {
        const escapedField = this.escapeIdentifier(field);
        // This is a simplified implementation - a more complete one would handle complex conditions
        setClauses.push(`${escapedField} = (
          SELECT jsonb_agg(elem)
          FROM jsonb_array_elements(COALESCE(${escapedField}, '[]'::jsonb)) elem
          WHERE elem != ${this.getParameterPlaceholder(params.length)}::jsonb
        )`);
        params.push(JSON.stringify(value));
      }
    }
    
    return setClauses.join(", ");
  }

  /**
   * Translates a MongoDB-style filter to a SQL WHERE clause.
   * 
   * @param filter The MongoDB-style filter
   * @param tableAlias Optional table alias to use in the SQL
   * @returns A SQL query object with the WHERE clause and parameters
   */
  override translateFilter<T>(filter: Filter<T>, tableAlias?: string): SqlQuery {
    const params: unknown[] = [];
    const prefix = tableAlias ? `${this.escapeIdentifier(tableAlias)}.` : '';
    const whereClause = this.buildWhereClause(filter, params, prefix);
    
    return {
      sql: whereClause ? `WHERE ${whereClause}` : '',
      params
    };
  }

  /**
   * Translates a MongoDB-style update to a SQL UPDATE statement.
   * 
   * @param tableName The name of the table to update
   * @param update The MongoDB-style update operators
   * @param filter The MongoDB-style filter to select documents to update
   * @returns A SQL query object with the UPDATE statement and parameters
   */
  override translateUpdate<T>(tableName: string, update: UpdateOperator<T>, filter: Filter<T>): SqlQuery {
    const params: unknown[] = [];
    const escapedTableName = this.escapeIdentifier(tableName);
    const setClause = this.buildSetClause(update, params);
    
    const whereQuery = this.translateFilter(filter);
    
    let sql = `UPDATE ${escapedTableName} SET ${setClause}`;
    
    if (whereQuery.sql) {
      sql += ` ${whereQuery.sql}`;
    }
    
    return { sql, params: [...params, ...whereQuery.params] };
  }

  /**
   * Translates a MongoDB-style filter to a SQL DELETE statement.
   * 
   * @param tableName The name of the table to delete from
   * @param filter The MongoDB-style filter to select documents to delete
   * @returns A SQL query object with the DELETE statement and parameters
   */
  override translateDelete<T>(tableName: string, filter: Filter<T>): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    
    const whereQuery = this.translateFilter(filter);
    
    let sql = `DELETE FROM ${escapedTableName}`;
    
    if (whereQuery.sql) {
      sql += ` ${whereQuery.sql}`;
    }
    
    return { sql, params: whereQuery.params };
  }

  /**
   * Translates a MongoDB-style find to a SQL SELECT statement.
   * 
   * @param tableName The name of the table
   * @param filter The filter
   * @param options The find options
   * @returns The SQL query
   */
  translateFind(
    tableName: string,
    filter: Filter<Document>,
    options?: {
      projection?: Record<string, 1 | 0>;
      sort?: Record<string, 1 | -1>;
      skip?: number;
      limit?: number;
    }
  ): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const whereQuery = this.translateFilter(filter);
    
    // Handle projection
    let selectClause = "*";
    if (options?.projection) {
      const includeFields = Object.entries(options.projection)
        .filter(([_, value]) => value === 1)
        .map(([field, _]) => field);
      
      const excludeFields = Object.entries(options.projection)
        .filter(([_, value]) => value === 0)
        .map(([field, _]) => field);
      
      if (includeFields.length > 0) {
        // Include only specified fields
        selectClause = includeFields
          .map(field => this.escapeIdentifier(field))
          .join(", ");
      } else if (excludeFields.length > 0) {
        // Exclude specified fields - this is more complex in SQL
        // For PostgreSQL, we can use JSON functions to build a custom object
        selectClause = `jsonb_build_object(${
          Object.keys(filter)
            .filter(field => !excludeFields.includes(field))
            .map(field => `'${field}', ${this.escapeIdentifier(field)}`)
            .join(", ")
        })`;
      }
    }
    
    let sql = `SELECT ${selectClause} FROM ${escapedTableName}`;
    
    if (whereQuery.sql) {
      sql += ` ${whereQuery.sql}`;
    }
    
    // Handle sort
    if (options?.sort && Object.keys(options.sort).length > 0) {
      const orderClauses = Object.entries(options.sort)
        .map(([field, direction]) => {
          const dir = direction === 1 ? "ASC" : "DESC";
          return `${this.escapeIdentifier(field)} ${dir}`;
        });
      
      sql += ` ORDER BY ${orderClauses.join(", ")}`;
    }
    
    // Handle pagination
    if (options?.limit) {
      sql += ` LIMIT ${options.limit}`;
    }
    
    if (options?.skip) {
      sql += ` OFFSET ${options.skip}`;
    }
    
    return { sql, params: whereQuery.params };
  }

  /**
   * Translates a MongoDB-style count to a SQL COUNT statement.
   * 
   * @param tableName The name of the table
   * @param filter The filter
   * @returns The SQL query
   */
  translateCount(tableName: string, filter: Filter<Document>): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const whereQuery = this.translateFilter(filter);
    
    let sql = `SELECT COUNT(*) as count FROM ${escapedTableName}`;
    
    if (whereQuery.sql) {
      sql += ` ${whereQuery.sql}`;
    }
    
    return { sql, params: whereQuery.params };
  }

  /**
   * Processes a SQL query result into a format compatible with MQLib.
   * 
   * @param result The raw SQL query result
   * @param tableName The name of the table being queried
   * @returns The processed result
   */
  override processQueryResult(result: any, tableName: string): { rows: Record<string, unknown>[]; rowCount: number } {
    // If the result is already in the expected format, return it as is
    if (result.rows && typeof result.rowCount === 'number') {
      return result;
    }
    
    // Handle PostgreSQL-specific result format
    if (result && result.rows) {
      const rows = result.rows.map((row: any) => {
        const processedRow: Record<string, unknown> = {};
        
        for (const [key, value] of Object.entries(row)) {
          // PostgreSQL already handles JSON types, but we'll process them anyway for consistency
          if (typeof value === 'string' && (
            value.startsWith('{') || 
            value.startsWith('[')
          )) {
            try {
              processedRow[key] = JSON.parse(value);
            } catch {
              processedRow[key] = value;
            }
          } else {
            processedRow[key] = value;
          }
        }
        
        return processedRow;
      });
      
      return {
        rows,
        rowCount: result.rowCount || rows.length
      };
    }
    
    // Default case: empty result
    return { rows: [], rowCount: 0 };
  }

  /**
   * Processes a document before inserting it into the database.
   * 
   * @param doc The document to process
   * @returns The processed document
   */
  override processDocumentForInsert<T extends Record<string, unknown>>(doc: T): Record<string, unknown> {
    // PostgreSQL has native JSON support, so we don't need to stringify objects
    return doc as Record<string, unknown>;
  }

  /**
   * Translates a MongoDB-style document to a SQL INSERT statement.
   * 
   * @param tableName The name of the table to insert into
   * @param document The document to insert
   * @returns A SQL query object with the INSERT statement and parameters
   */
  override translateInsert<T>(tableName: string, document: T): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const params: unknown[] = [];
    
    // Ensure document is treated as an object
    const doc = document as unknown as Record<string, unknown>;
    const keys = Object.keys(doc);
    const escapedKeys = keys.map(key => this.escapeIdentifier(key));
    
    const valuePlaceholders = keys.map((_, i) => this.getParameterPlaceholder(i));
    
    for (const key of keys) {
      params.push(doc[key]);
    }
    
    const sql = `INSERT INTO ${escapedTableName} (${escapedKeys.join(", ")})
      VALUES (${valuePlaceholders.join(", ")})
      RETURNING *`;
    
    return { sql, params };
  }
} 