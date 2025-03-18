/**
 * MySQL adapter for MQLib
 * Implements the SqlAdapter interface for MySQL
 */

import { SqlAdapter, SqlQuery } from "../../interfaces/adapter.ts";
import { SqlDialect } from "../../types/sql.ts";
import { UpdateOperator } from "../../types/update.ts";
import { Filter } from "../../types/filter.ts";
import { Document } from "../../types/document.ts";
import { BaseSqlAdapter } from "../base_adapter.ts";

/**
 * MySQL adapter implementation
 */
export class MySqlAdapter extends BaseSqlAdapter {
  /**
   * The SQL dialect
   */
  readonly dialect = SqlDialect.MYSQL;

  /**
   * Escapes an identifier (table name, column name) for MySQL
   * @param identifier The identifier to escape
   * @returns The escaped identifier
   */
  override escapeIdentifier(identifier: string): string {
    return `\`${identifier.replace(/`/g, '``')}\``;
  }

  /**
   * Gets the parameter placeholder for MySQL (?)
   * @param _index The parameter index (0-based) - not used in MySQL
   * @returns The parameter placeholder
   */
  override getParameterPlaceholder(_index: number): string {
    return "?";
  }

  /**
   * Translates a JSON schema to a MySQL CREATE TABLE statement
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
      let sqlType = "VARCHAR(255)";
      let constraints = "";
      
      if (key === "_id") {
        constraints = " PRIMARY KEY";
      }
      
      if (required.includes(key)) {
        constraints += " NOT NULL";
      }
      
      switch (propDef.type) {
        case "string":
          if (propDef.maxLength) {
            sqlType = `VARCHAR(${propDef.maxLength})`;
          } else {
            sqlType = "TEXT";
          }
          break;
        case "integer":
          sqlType = "INT";
          break;
        case "number":
          sqlType = "DOUBLE";
          break;
        case "boolean":
          sqlType = "BOOLEAN";
          break;
        case "object":
          sqlType = "JSON";
          break;
        case "array":
          sqlType = "JSON";
          break;
      }
      
      columns.push(`${this.escapeIdentifier(key)} ${sqlType}${constraints}`);
    }
    
    const sql = `CREATE TABLE IF NOT EXISTS ${escapedTableName} (
      ${columns.join(",\n      ")}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`;
    
    return { sql, params: [] };
  }

  /**
   * Translates a create index operation to a MySQL CREATE INDEX statement
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
    
    const sql = `CREATE ${unique}INDEX ${escapedIndexName} ON ${escapedTableName} (${indexFields.join(", ")})`;
    
    return { sql, params: [] };
  }

  /**
   * Builds SQL conditions for MongoDB-style operators.
   * 
   * @param column The column name
   * @param operators The operators
   * @param params The parameters array to append to
   * @returns The SQL condition
   */
  protected override buildOperatorConditions(column: string, operators: Record<string, unknown>, params: unknown[]): string {
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
            const placeholders = value.map(() => this.getParameterPlaceholder(0));
            conditions.push(`${escapedColumn} IN (${placeholders.join(", ")})`);
            params.push(...value);
          } else {
            conditions.push("FALSE"); // Empty $in array should never match
          }
          break;
        case "$nin":
          if (Array.isArray(value) && value.length > 0) {
            const placeholders = value.map(() => this.getParameterPlaceholder(0));
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
          conditions.push(`${escapedColumn} REGEXP ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$like":
          conditions.push(`${escapedColumn} LIKE ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
          break;
        case "$contains":
          // For JSON arrays in MySQL
          conditions.push(`JSON_CONTAINS(${escapedColumn}, ${this.getParameterPlaceholder(params.length)})`);
          params.push(JSON.stringify(value));
          break;
        case "$containsAny":
          // For JSON arrays in MySQL - this is a simplified implementation
          if (Array.isArray(value)) {
            const orConditions = value.map(() => {
              const placeholder = this.getParameterPlaceholder(params.length);
              params.push(JSON.stringify(value));
              return `JSON_CONTAINS(${escapedColumn}, ${placeholder})`;
            });
            conditions.push(`(${orConditions.join(" OR ")})`);
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
  protected override buildSetClause<T>(update: UpdateOperator<T>, params: unknown[]): string {
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
    
    // Handle $push operator (for JSON arrays)
    if (update.$push) {
      for (const [field, value] of Object.entries(update.$push)) {
        const escapedField = this.escapeIdentifier(field);
        setClauses.push(`${escapedField} = JSON_ARRAY_APPEND(
          COALESCE(${escapedField}, JSON_ARRAY()), 
          '$', 
          ${this.getParameterPlaceholder(params.length)}
        )`);
        params.push(value);
      }
    }
    
    // Handle $pull operator (for JSON arrays)
    if (update.$pull) {
      for (const [field, value] of Object.entries(update.$pull)) {
        const escapedField = this.escapeIdentifier(field);
        // This is a simplified implementation for MySQL
        // A more complete one would use JSON_REMOVE with the correct path
        setClauses.push(`${escapedField} = (
          SELECT JSON_ARRAYAGG(elem)
          FROM JSON_TABLE(
            COALESCE(${escapedField}, JSON_ARRAY()),
            '$[*]' COLUMNS(elem JSON PATH '$')
          ) AS jt
          WHERE JSON_EXTRACT(elem, '$') != ${this.getParameterPlaceholder(params.length)}
        )`);
        params.push(JSON.stringify(value));
      }
    }
    
    return setClauses.join(", ");
  }

  /**
   * Translates a MongoDB-style filter to a SQL WHERE clause
   * @param filter The filter
   * @param tableAlias Optional table alias to use in the SQL
   * @returns The SQL query with WHERE clause
   */
  override translateFilter<T>(filter: Filter<T>, tableAlias?: string): SqlQuery {
    const params: unknown[] = [];
    let whereClause = "";

    if (filter && Object.keys(filter).length > 0) {
      const conditions: string[] = [];
      const prefix = tableAlias ? `${this.escapeIdentifier(tableAlias)}.` : '';

      for (const [key, value] of Object.entries(filter)) {
        // Handle logical operators
        if (key === "$and" && Array.isArray(value)) {
          const andConditions = value.map(subFilter => {
            const subQuery = this.translateFilter(subFilter, tableAlias);
            params.push(...subQuery.params);
            return subQuery.sql;
          }).filter(sql => sql !== "");
          
          if (andConditions.length > 0) {
            conditions.push(`(${andConditions.join(" AND ")})`);
          }
          continue;
        }

        if (key === "$or" && Array.isArray(value)) {
          const orConditions = value.map(subFilter => {
            const subQuery = this.translateFilter(subFilter, tableAlias);
            params.push(...subQuery.params);
            return subQuery.sql;
          }).filter(sql => sql !== "");
          
          if (orConditions.length > 0) {
            conditions.push(`(${orConditions.join(" OR ")})`);
          }
          continue;
        }

        if (key === "$nor" && Array.isArray(value)) {
          const norConditions = value.map(subFilter => {
            const subQuery = this.translateFilter(subFilter, tableAlias);
            params.push(...subQuery.params);
            return subQuery.sql;
          }).filter(sql => sql !== "");
          
          if (norConditions.length > 0) {
            conditions.push(`NOT (${norConditions.join(" OR ")})`);
          }
          continue;
        }

        // Handle regular field conditions
        if (typeof value === "object" && value !== null && !Array.isArray(value)) {
          // This is an operator object like { $gt: 5, $lt: 10 }
          const column = prefix + key;
          conditions.push(this.buildOperatorConditions(column, value as Record<string, unknown>, params));
        } else {
          // This is a direct equality check
          const column = prefix + key;
          const escapedColumn = this.escapeIdentifier(column);
          conditions.push(`${escapedColumn} = ${this.getParameterPlaceholder(params.length)}`);
          params.push(value);
        }
      }

      whereClause = conditions.join(" AND ");
    }

    return { sql: whereClause, params };
  }

  /**
   * Translates a MongoDB-style update to a SQL UPDATE statement
   * @param tableName The name of the table to update
   * @param update The update operations
   * @param filter The filter to select documents to update
   * @returns The SQL query
   */
  override translateUpdate<T>(tableName: string, update: UpdateOperator<T>, filter: Filter<T>): SqlQuery {
    const params: unknown[] = [];
    const escapedTableName = this.escapeIdentifier(tableName);
    const setClause = this.buildSetClause(update, params);
    
    const whereQuery = this.translateFilter(filter);
    params.push(...whereQuery.params);
    
    let sql = `UPDATE ${escapedTableName} SET ${setClause}`;
    
    if (whereQuery.sql) {
      sql += ` WHERE ${whereQuery.sql}`;
    }
    
    return { sql, params };
  }

  /**
   * Translates a MongoDB-style document to a SQL INSERT statement
   * @param tableName The name of the table to insert into
   * @param document The document to insert
   * @returns The SQL query
   */
  override translateInsert<T>(tableName: string, document: T): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const params: unknown[] = [];
    
    // Ensure document is treated as an object
    const doc = document as unknown as Record<string, unknown>;
    const keys = Object.keys(doc);
    const escapedKeys = keys.map(key => this.escapeIdentifier(key));
    
    const valuePlaceholders = keys.map(() => this.getParameterPlaceholder(0));
    
    for (const key of keys) {
      params.push(doc[key]);
    }
    
    const sql = `INSERT INTO ${escapedTableName} (${escapedKeys.join(", ")})
      VALUES (${valuePlaceholders.join(", ")})`;
    
    return { sql, params };
  }

  /**
   * Translates a MongoDB-style filter to a SQL DELETE statement
   * @param tableName The name of the table to delete from
   * @param filter The filter to select documents to delete
   * @returns The SQL query
   */
  override translateDelete<T>(tableName: string, filter: Filter<T>): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    
    const whereQuery = this.translateFilter(filter);
    
    let sql = `DELETE FROM ${escapedTableName}`;
    
    if (whereQuery.sql) {
      sql += ` WHERE ${whereQuery.sql}`;
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
    
    // Handle MySQL-specific result format
    if (Array.isArray(result)) {
      // MySQL might return an array of rows
      const rows = result.map((row: any) => {
        const processedRow: Record<string, unknown> = {};
        
        for (const [key, value] of Object.entries(row)) {
          // Try to parse JSON strings
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
        rowCount: rows.length
      };
    }
    
    // Handle result with affectedRows property (for INSERT, UPDATE, DELETE)
    if (result && typeof result.affectedRows === 'number') {
      return {
        rows: [],
        rowCount: result.affectedRows
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
    const processedDoc: Record<string, unknown> = {};
    
    for (const [key, value] of Object.entries(doc)) {
      if (typeof value === 'object' && value !== null) {
        // Convert objects and arrays to JSON strings
        processedDoc[key] = JSON.stringify(value);
      } else {
        processedDoc[key] = value;
      }
    }
    
    return processedDoc;
  }
} 