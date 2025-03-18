/**
 * SQL-related types for MQLib
 */

/**
 * Represents a SQL query with parameterized values
 */
export interface SqlQuery {
  /**
   * The SQL query string with parameter placeholders
   */
  sql: string;
  
  /**
   * The parameter values to be used with the query
   */
  params: unknown[];
}

/**
 * Supported SQL dialects
 */
export enum SqlDialect {
  SQLITE = "sqlite",
  POSTGRES = "postgres",
  MYSQL = "mysql"
} 