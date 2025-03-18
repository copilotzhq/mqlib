/**
 * Base SQL Adapter implementation for MQLib
 */

import { SqlAdapter, SqlQuery } from "../interfaces/adapter.ts";
import { Filter } from "../types/filter.ts";
import { UpdateOperator } from "../types/update.ts";

/**
 * Base schema-related interfaces that can be used by all adapters
 */
export interface FieldDefinition {
  type: string;
  isScalar: boolean;
  sqlType: string;
  path: string;
  isArray?: boolean;
}

export interface CollectionSchema {
  tableName: string;
  fields: Map<string, FieldDefinition>;
  hasExtraField: boolean;
}

/**
 * Base implementation of the SqlAdapter interface.
 * 
 * This class provides common functionality for all SQL dialect adapters
 * and should be extended by specific dialect implementations.
 */
export abstract class BaseSqlAdapter implements SqlAdapter {
  /**
   * Gets the name of the SQL dialect.
   */
  abstract readonly dialect: string;

  /**
   * Store collection schemas for reference
   */
  protected collectionSchemas: Map<string, CollectionSchema> = new Map();

  /**
   * Flag to indicate if schema-based approach is enabled
   */
  protected isSchemaEnabled = false;

  /**
   * Enables or disables the schema-based approach for this adapter.
   */
  public setSchemaEnabled(enabled: boolean): void {
    this.isSchemaEnabled = enabled;
  }

  /**
   * Gets the schema for a collection or table.
   * 
   * @param tableName The name of the table/collection
   * @returns The schema if available, undefined otherwise
   */
  public getCollectionSchema(tableName: string): CollectionSchema | undefined {
    return this.collectionSchemas.get(tableName);
  }

  /**
   * Translates a MongoDB-style filter to a SQL WHERE clause.
   * 
   * @param filter The MongoDB-style filter
   * @param tableAlias Optional table alias to use in the SQL
   * @returns A SQL query object with the WHERE clause and parameters
   */
  translateFilter<T>(filter: Filter<T>, tableAlias?: string): SqlQuery {
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
  translateUpdate<T>(tableName: string, update: UpdateOperator<T>, filter: Filter<T>): SqlQuery {
    const params: unknown[] = [];
    const setClause = this.buildSetClause(update, params);
    const whereQuery = this.translateFilter(filter);
    
    const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} ${whereQuery.sql}`;
    
    return {
      sql,
      params: [...params, ...whereQuery.params]
    };
  }

  /**
   * Translates a MongoDB-style document to a SQL INSERT statement.
   * 
   * @param tableName The name of the table to insert into
   * @param document The document to insert
   * @returns A SQL query object with the INSERT statement and parameters
   */
  translateInsert<T>(tableName: string, document: T): SqlQuery {
    const params: unknown[] = [];
    const columns: string[] = [];
    const placeholders: string[] = [];
    
    Object.entries(document as Record<string, unknown>).forEach(([key, value], index) => {
      // Skip undefined values
      if (value === undefined) return;
      
      columns.push(this.escapeIdentifier(key));
      placeholders.push(this.getParameterPlaceholder(index));
      params.push(this.serializeValue(value));
    });
    
    const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
    
    return { sql, params };
  }

  /**
   * Translates a MongoDB-style filter to a SQL DELETE statement.
   * 
   * @param tableName The name of the table to delete from
   * @param filter The MongoDB-style filter to select documents to delete
   * @returns A SQL query object with the DELETE statement and parameters
   */
  translateDelete<T>(tableName: string, filter: Filter<T>): SqlQuery {
    const whereQuery = this.translateFilter(filter);
    
    const sql = `DELETE FROM ${this.escapeIdentifier(tableName)} ${whereQuery.sql}`;
    
    return {
      sql,
      params: whereQuery.params
    };
  }

  /**
   * Translates a JSON schema to a SQL CREATE TABLE statement.
   * 
   * @param tableName The name of the table to create
   * @param schema The JSON schema defining the table structure
   * @returns A SQL query object with the CREATE TABLE statement
   */
  abstract translateSchema(tableName: string, schema: object): SqlQuery;

  /**
   * Translates a MongoDB-style index specification to a SQL CREATE INDEX statement.
   * 
   * @param tableName The name of the table to create an index on
   * @param indexName The name of the index
   * @param fields The fields to include in the index and their sort direction
   * @param options Options for the index
   * @returns A SQL query object with the CREATE INDEX statement
   */
  abstract translateCreateIndex(
    tableName: string,
    indexName: string,
    fields: Record<string, 1 | -1>,
    options?: { unique?: boolean; sparse?: boolean }
  ): SqlQuery;

  /**
   * Escapes a SQL identifier (table name, column name, etc.).
   * 
   * @param identifier The identifier to escape
   * @returns The escaped identifier
   */
  abstract escapeIdentifier(identifier: string): string;

  /**
   * Gets the placeholder syntax for parameterized queries.
   * 
   * @param index The parameter index (0-based)
   * @returns The placeholder string for the specified parameter
   */
  abstract getParameterPlaceholder(index: number): string;

  /**
   * Builds a SQL WHERE clause from a MongoDB-style filter.
   * 
   * @param filter The MongoDB-style filter
   * @param params Array to collect parameter values
   * @param prefix Optional prefix for column names (e.g., table alias)
   * @returns The SQL WHERE clause
   */
  protected buildWhereClause<T>(filter: Filter<T>, params: unknown[], prefix = ''): string {
    if (!filter || Object.keys(filter).length === 0) {
      return '';
    }

    const conditions: string[] = [];

    for (const [key, value] of Object.entries(filter)) {
      // Handle logical operators
      if (key === '$and' && Array.isArray(value)) {
        const andConditions = value.map(subFilter => this.buildWhereClause(subFilter, params, prefix)).filter(Boolean);
        if (andConditions.length > 0) {
          conditions.push(`(${andConditions.join(' AND ')})`);
        }
        continue;
      }

      if (key === '$or' && Array.isArray(value)) {
        const orConditions = value.map(subFilter => this.buildWhereClause(subFilter, params, prefix)).filter(Boolean);
        if (orConditions.length > 0) {
          conditions.push(`(${orConditions.join(' OR ')})`);
        }
        continue;
      }

      if (key === '$nor' && Array.isArray(value)) {
        const norConditions = value.map(subFilter => this.buildWhereClause(subFilter, params, prefix)).filter(Boolean);
        if (norConditions.length > 0) {
          conditions.push(`NOT (${norConditions.join(' OR ')})`);
        }
        continue;
      }

      // Handle regular field conditions
      const column = prefix + this.escapeIdentifier(key);

      if (value === null) {
        conditions.push(`${column} IS NULL`);
        continue;
      }

      if (typeof value !== 'object' || value instanceof Date) {
        // Simple equality
        params.push(this.serializeValue(value));
        conditions.push(`${column} = ${this.getParameterPlaceholder(params.length - 1)}`);
        continue;
      }

      // Handle operators
      const operatorConditions = this.buildOperatorConditions(column, value as Record<string, unknown>, params);
      if (operatorConditions) {
        conditions.push(operatorConditions);
      }
    }

    return conditions.join(' AND ');
  }

  /**
   * Builds SQL conditions for MongoDB-style operators.
   * 
   * @param column The column name
   * @param operators The operators and their values
   * @param params Array to collect parameter values
   * @returns The SQL conditions for the operators
   */
  protected buildOperatorConditions(column: string, operators: Record<string, unknown>, params: unknown[]): string {
    const conditions: string[] = [];

    for (const [op, value] of Object.entries(operators)) {
      switch (op) {
        case '$eq':
          if (value === null) {
            conditions.push(`${column} IS NULL`);
          } else {
            params.push(this.serializeValue(value));
            conditions.push(`${column} = ${this.getParameterPlaceholder(params.length - 1)}`);
          }
          break;

        case '$ne':
          if (value === null) {
            conditions.push(`${column} IS NOT NULL`);
          } else {
            params.push(this.serializeValue(value));
            conditions.push(`${column} <> ${this.getParameterPlaceholder(params.length - 1)}`);
          }
          break;

        case '$gt':
          params.push(this.serializeValue(value));
          conditions.push(`${column} > ${this.getParameterPlaceholder(params.length - 1)}`);
          break;

        case '$gte':
          params.push(this.serializeValue(value));
          conditions.push(`${column} >= ${this.getParameterPlaceholder(params.length - 1)}`);
          break;

        case '$lt':
          params.push(this.serializeValue(value));
          conditions.push(`${column} < ${this.getParameterPlaceholder(params.length - 1)}`);
          break;

        case '$lte':
          params.push(this.serializeValue(value));
          conditions.push(`${column} <= ${this.getParameterPlaceholder(params.length - 1)}`);
          break;

        case '$in':
          if (Array.isArray(value) && value.length > 0) {
            const placeholders: string[] = [];
            for (const item of value) {
              params.push(this.serializeValue(item));
              placeholders.push(this.getParameterPlaceholder(params.length - 1));
            }
            conditions.push(`${column} IN (${placeholders.join(', ')})`);
          } else {
            // Empty $in array should never match anything
            conditions.push('FALSE');
          }
          break;

        case '$nin':
          if (Array.isArray(value) && value.length > 0) {
            const placeholders: string[] = [];
            for (const item of value) {
              params.push(this.serializeValue(item));
              placeholders.push(this.getParameterPlaceholder(params.length - 1));
            }
            conditions.push(`${column} NOT IN (${placeholders.join(', ')})`);
          } else {
            // Empty $nin array should match everything
            conditions.push('TRUE');
          }
          break;

        case '$exists':
          conditions.push(value ? `${column} IS NOT NULL` : `${column} IS NULL`);
          break;

        case '$regex':
          // This will need to be overridden by dialect-specific implementations
          params.push(this.serializeValue(value));
          conditions.push(`${column} REGEXP ${this.getParameterPlaceholder(params.length - 1)}`);
          break;

        // Array operators and other complex operators will need dialect-specific implementations
        case '$all':
        case '$elemMatch':
        case '$size':
        case '$type':
        case '$mod':
          throw new Error(`Operator ${op} not implemented in base adapter`);
      }
    }

    return conditions.length > 0 ? `(${conditions.join(' AND ')})` : '';
  }

  /**
   * Builds a SQL SET clause from a MongoDB-style update.
   * 
   * @param update The MongoDB-style update operators
   * @param params Array to collect parameter values
   * @returns The SQL SET clause
   */
  protected buildSetClause<T>(update: UpdateOperator<T>, params: unknown[]): string {
    const setClauses: string[] = [];

    // Handle $set operator
    if (update.$set) {
      for (const [key, value] of Object.entries(update.$set)) {
        // Handle nested paths
        if (key.includes('.')) {
          this.buildNestedSetClause(key, value, setClauses, params);
        } else {
          params.push(this.serializeValue(value));
          setClauses.push(`${this.escapeIdentifier(key)} = ${this.getParameterPlaceholder(params.length - 1)}`);
        }
      }
    }

    // Handle $inc operator
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        params.push(value);
        setClauses.push(`${this.escapeIdentifier(key)} = ${this.escapeIdentifier(key)} + ${this.getParameterPlaceholder(params.length - 1)}`);
      }
    }

    // Handle $mul operator
    if (update.$mul) {
      for (const [key, value] of Object.entries(update.$mul)) {
        params.push(value);
        setClauses.push(`${this.escapeIdentifier(key)} = ${this.escapeIdentifier(key)} * ${this.getParameterPlaceholder(params.length - 1)}`);
      }
    }

    // Handle $unset operator (set fields to NULL)
    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        setClauses.push(`${this.escapeIdentifier(key)} = NULL`);
      }
    }

    // Handle $min operator
    if (update.$min) {
      for (const [key, value] of Object.entries(update.$min)) {
        params.push(this.serializeValue(value));
        setClauses.push(`${this.escapeIdentifier(key)} = CASE WHEN ${this.escapeIdentifier(key)} > ${this.getParameterPlaceholder(params.length - 1)} OR ${this.escapeIdentifier(key)} IS NULL THEN ${this.getParameterPlaceholder(params.length - 1)} ELSE ${this.escapeIdentifier(key)} END`);
      }
    }

    // Handle $max operator
    if (update.$max) {
      for (const [key, value] of Object.entries(update.$max)) {
        params.push(this.serializeValue(value));
        setClauses.push(`${this.escapeIdentifier(key)} = CASE WHEN ${this.escapeIdentifier(key)} < ${this.getParameterPlaceholder(params.length - 1)} OR ${this.escapeIdentifier(key)} IS NULL THEN ${this.getParameterPlaceholder(params.length - 1)} ELSE ${this.escapeIdentifier(key)} END`);
      }
    }

    // Handle Array Operators
    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        this.buildPushOperation(key, serializedValue, setClauses, params);
      }
    }

    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        this.buildAddToSetOperation(key, serializedValue, setClauses, params);
      }
    }

    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        this.buildPullOperation(key, serializedValue, setClauses, params);
      }
    }

    return setClauses.join(', ');
  }

  /**
   * Builds the SET clause for a nested field path like "address.city" or "skills.1.level"
   * 
   * @param path The field path
   * @param value The value to set
   * @param setClauses Array to collect SET clauses
   * @param params Array to collect parameter values
   */
  protected buildNestedSetClause(path: string, value: unknown, setClauses: string[], params: unknown[]): void {
    const parts = path.split('.');
    const rootField = parts[0];
    
    // Check if the second part is a number (array index)
    if (parts.length >= 2 && !isNaN(Number(parts[1]))) {
      // We're updating an array element at a specific index
      const arrayIndex = Number(parts[1]);
      
      if (parts.length === 2) {
        // Replace the entire array element
        this.buildArrayElementSetClause(rootField, arrayIndex, null, value, setClauses, params);
      } else {
        // Replace a property within an array element
        const propertyPath = parts.slice(2).join('.');
        this.buildArrayElementSetClause(rootField, arrayIndex, propertyPath, value, setClauses, params);
      }
    } else {
      // Regular nested path like "address.city"
      this.buildObjectPropertySetClause(rootField, parts.slice(1).join('.'), value, setClauses, params);
    }
  }

  /**
   * Builds a SET clause for an array element at a specific index
   * This is dialect-specific and must be implemented by each adapter
   */
  protected abstract buildArrayElementSetClause(
    field: string,
    index: number, 
    propertyPath: string | null,
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void;

  /**
   * Builds a SET clause for a nested object property
   * This is dialect-specific and must be implemented by each adapter
   */
  protected abstract buildObjectPropertySetClause(
    field: string,
    propertyPath: string,
    value: unknown,
    setClauses: string[],
    params: unknown[]
  ): void;

  /**
   * Builds a SET clause for a $push operation
   * This is dialect-specific and must be implemented by each adapter
   */
  protected abstract buildPushOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void;

  /**
   * Builds a SET clause for an $addToSet operation
   * This is dialect-specific and must be implemented by each adapter
   */
  protected abstract buildAddToSetOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void;

  /**
   * Builds a SET clause for a $pull operation
   * This is dialect-specific and must be implemented by each adapter
   */
  protected abstract buildPullOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void;

  /**
   * Checks if a field is an array field
   * This is dialect-specific and depends on schema information
   */
  protected abstract isArrayField(tableName: string, fieldName: string): boolean;

  /**
   * Checks if a field is a complex object field
   * This is dialect-specific and depends on schema information
   */
  protected abstract isComplexObjectField(tableName: string, fieldName: string): boolean;

  /**
   * Helper method to recursively parse JSON strings in objects and arrays
   * 
   * @param value The value to parse 
   * @returns The parsed value
   */
  protected parseJsonRecursively(value: unknown): unknown {
    if (typeof value === 'string') {
      // Skip special strings that might look like JSON but shouldn't be parsed
      if (this.shouldSkipJsonParsing(value)) {
        return value;
      }

      try {
        return this.parseJsonRecursively(JSON.parse(value));
      } catch (e) {
        // If it's not valid JSON, return the original string
        return value;
      }
    } else if (Array.isArray(value)) {
      // Parse each element in the array
      return value.map(item => this.parseJsonRecursively(item));
    } else if (value !== null && typeof value === 'object') {
      // Parse each value in the object
      const result: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(value)) {
        // Handle special field transformations
        result[key] = this.transformFieldValue(key, this.parseJsonRecursively(val));
      }
      return result;
    }
    
    return value;
  }

  /**
   * Checks if a string value should be skipped for JSON parsing
   * This can be overridden by specific adapters
   */
  protected shouldSkipJsonParsing(value: string): boolean {
    // By default, skip UUID-like strings and _id fields
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
  }

  /**
   * Transform a field value based on field name or other criteria
   * This can be overridden by specific adapters
   */
  protected transformFieldValue(fieldName: string, value: unknown): unknown {
    // Default implementation: return value as is
    return value;
  }

  /**
   * Checks if a string might be JSON data
   */
  protected mightBeJsonData(value: string): boolean {
    return (value.startsWith('{') && value.endsWith('}')) || 
           (value.startsWith('[') && value.endsWith(']'));
  }

  /**
   * Handles nested paths in filter objects
   * Converts dot notation like address.city to nested objects
   */
  protected handleNestedPathInFilter<T>(filter: Filter<T>): Filter<T> {
    if (!filter || typeof filter !== 'object') {
      return filter;
    }

    const result: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(filter)) {
      // Skip MongoDB operators
      if (key.startsWith('$')) {
        result[key] = this.handleNestedPathInArray(value);
        continue;
      }

      // Handle dot notation
      if (key.includes('.')) {
        const parts = key.split('.');
        let current = result;
        
        // Build the nested structure
        for (let i = 0; i < parts.length - 1; i++) {
          const part = parts[i];
          if (!(part in current)) {
            current[part] = {};
          }
          current = current[part] as Record<string, unknown>;
        }
        
        // Set the value at the deepest level
        const lastPart = parts[parts.length - 1];
        current[lastPart] = value;
      } else {
        // Regular field
        result[key] = this.processFilterValue(value);
      }
    }

    return result as Filter<T>;
  }

  /**
   * Processes values in arrays for nested path handling
   */
  private handleNestedPathInArray(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map(item => this.processFilterValue(item));
    }
    return this.processFilterValue(value);
  }

  /**
   * Processes a filter value, handling nested paths in objects
   */
  private processFilterValue(value: unknown): unknown {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return this.handleNestedPathInFilter(value as Filter<unknown>);
    }
    return value;
  }

  /**
   * Serializes a value for use in a SQL query.
   * 
   * @param value The value to serialize
   * @returns The serialized value
   */
  protected serializeValue(value: unknown): unknown {
    if (value === null || value === undefined) {
      return null;
    }

    if (value instanceof Date) {
      return value.toISOString();
    }

    if (typeof value === 'object') {
      return JSON.stringify(value);
    }

    return value;
  }

  /**
   * Processes a SQL query result into a format compatible with MQLib.
   * 
   * @param result The raw SQL query result
   * @param tableName The name of the table being queried
   * @returns The processed result with rows and rowCount
   */
  processQueryResult(result: any, tableName: string): { rows: Record<string, unknown>[]; rowCount: number } {
    // Default implementation - should be overridden by specific adapters
    if (result && result.rows && typeof result.rowCount === 'number') {
      return result;
    }
    
    // Default case: empty result
    return { rows: [], rowCount: 0 };
  }

  /**
   * Processes a document before inserting it into the database.
   * 
   * @param doc The document to process
   * @param tableName The name of the table to insert into
   * @returns The processed document
   */
  processDocumentForInsert<T extends Record<string, unknown>>(doc: T, tableName: string): Record<string, unknown> {
    // Default implementation - should be overridden by specific adapters
    return { ...doc };
  }

  /**
   * Translates a MongoDB-style find operation to a SQL SELECT statement.
   * 
   * @param tableName The name of the table to select from
   * @param filter The MongoDB-style filter to select documents
   * @param options Options for the find operation
   * @returns A SQL query object with the SELECT statement and parameters
   */
  translateFind<T>(tableName: string, filter: Filter<T>, options: any = {}): SqlQuery {
    // Start with SELECT * FROM table
    const selectClause = '*';
    const params: unknown[] = [];
    
    // Add the WHERE clause
    const whereQuery = this.translateFilter(filter);
    let sql = `SELECT ${selectClause} FROM ${this.escapeIdentifier(tableName)} ${whereQuery.sql}`;
    params.push(...whereQuery.params);
    
    // Add sorting
    if (options.sort) {
      const sortClauses: string[] = [];
      
      for (const [field, direction] of Object.entries(options.sort)) {
        sortClauses.push(`${this.escapeIdentifier(field)} ${direction === 1 ? 'ASC' : 'DESC'}`);
      }
      
      if (sortClauses.length > 0) {
        sql += ` ORDER BY ${sortClauses.join(', ')}`;
      }
    }
    
    // Add pagination
    if (options.limit !== undefined) {
      sql += ` LIMIT ${this.getParameterPlaceholder(params.length)}`;
      params.push(options.limit);
    }
    
    if (options.skip !== undefined) {
      sql += ` OFFSET ${this.getParameterPlaceholder(params.length)}`;
      params.push(options.skip);
    }
    
    return { sql, params };
  }
} 