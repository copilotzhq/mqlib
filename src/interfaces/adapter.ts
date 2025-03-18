/**
 * SQL Adapter interface for MQLib
 */

import { Filter } from "../types/filter.ts";
import { UpdateOperator } from "../types/update.ts";

/**
 * Represents a SQL query with parameterized values.
 */
export interface SqlQuery {
  /** The SQL query string with placeholders */
  sql: string;
  /** The parameter values for the placeholders */
  params: unknown[];
}

/**
 * Represents a SQL adapter for a specific database dialect.
 * 
 * This interface defines methods for translating MongoDB-style operations
 * to SQL queries for a specific database dialect.
 */
export interface SqlAdapter {
  /**
   * Gets the name of the SQL dialect.
   */
  readonly dialect: string;

  /**
   * Translates a MongoDB-style filter to a SQL WHERE clause.
   * 
   * @param filter The MongoDB-style filter
   * @param tableAlias Optional table alias to use in the SQL
   * @returns A SQL query object with the WHERE clause and parameters
   */
  translateFilter<T>(filter: Filter<T>, tableAlias?: string): SqlQuery;

  /**
   * Translates a MongoDB-style update to a SQL UPDATE statement.
   * 
   * @param tableName The name of the table to update
   * @param update The MongoDB-style update operators
   * @param filter The MongoDB-style filter to select documents to update
   * @returns A SQL query object with the UPDATE statement and parameters
   */
  translateUpdate<T>(tableName: string, update: UpdateOperator<T>, filter: Filter<T>): SqlQuery;

  /**
   * Translates a MongoDB-style document to a SQL INSERT statement.
   * 
   * @param tableName The name of the table to insert into
   * @param document The document to insert
   * @returns A SQL query object with the INSERT statement and parameters
   */
  translateInsert<T>(tableName: string, document: T): SqlQuery;

  /**
   * Translates a MongoDB-style filter to a SQL DELETE statement.
   * 
   * @param tableName The name of the table to delete from
   * @param filter The MongoDB-style filter to select documents to delete
   * @returns A SQL query object with the DELETE statement and parameters
   */
  translateDelete<T>(tableName: string, filter: Filter<T>): SqlQuery;

  /**
   * Translates a JSON schema to a SQL CREATE TABLE statement.
   * 
   * @param tableName The name of the table to create
   * @param schema The JSON schema defining the table structure
   * @returns A SQL query object with the CREATE TABLE statement
   */
  translateSchema(tableName: string, schema: object): SqlQuery;

  /**
   * Translates a MongoDB-style index specification to a SQL CREATE INDEX statement.
   * 
   * @param tableName The name of the table to create an index on
   * @param indexName The name of the index
   * @param fields The fields to include in the index and their sort direction
   * @param options Options for the index
   * @returns A SQL query object with the CREATE INDEX statement
   */
  translateCreateIndex(
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
  escapeIdentifier(identifier: string): string;

  /**
   * Gets the placeholder syntax for parameterized queries.
   * 
   * @param index The parameter index (0-based)
   * @returns The placeholder string for the specified parameter
   */
  getParameterPlaceholder(index: number): string;

  /**
   * Processes a SQL query result into a format compatible with MQLib.
   * 
   * @param result The raw SQL query result
   * @param tableName The name of the table being queried
   * @returns The processed result with rows and rowCount
   */
  processQueryResult(result: any, tableName: string): { rows: Record<string, unknown>[]; rowCount: number };

  /**
   * Processes a document before inserting it into the database.
   * 
   * @param doc The document to process
   * @param tableName The name of the table to insert into
   * @returns The processed document
   */
  processDocumentForInsert<T extends Record<string, unknown>>(doc: T, tableName: string): Record<string, unknown>;
} 