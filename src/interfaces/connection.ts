/**
 * Connection interface for MQLib
 */

import { Database } from "./database.ts";

/**
 * Options for database connections.
 */
export interface ConnectionOptions {
  /** The host to connect to */
  host?: string;
  /** The port to connect to */
  port?: number;
  /** The username for authentication */
  username?: string;
  /** The password for authentication */
  password?: string;
  /** The database name to use */
  database?: string;
  /** Connection pool size */
  poolSize?: number;
  /** Connection timeout in milliseconds */
  connectionTimeout?: number;
  /** Whether to use SSL/TLS */
  ssl?: boolean;
  /** Additional driver-specific options */
  [key: string]: unknown;
}

/**
 * Represents a connection to a database server.
 */
export interface Connection {
  /**
   * Gets the connection status.
   */
  readonly isConnected: boolean;

  /**
   * Connects to the database server.
   * 
   * @returns A promise that resolves when the connection is established
   */
  connect(): Promise<void>;

  /**
   * Closes the connection to the database server.
   * 
   * @returns A promise that resolves when the connection is closed
   */
  close(): Promise<void>;

  /**
   * Gets a database instance.
   * 
   * @param name The name of the database
   * @returns A Database instance for the specified name
   */
  db(name: string): Database;

  /**
   * Lists all databases on the server.
   * 
   * @returns A promise that resolves to an array of database names
   */
  listDatabases(): Promise<string[]>;

  /**
   * Executes a raw SQL query.
   * 
   * This is an escape hatch for operations that aren't supported by the MongoDB-like API.
   * 
   * @param sql The SQL query to execute
   * @param params Parameters for the query
   * @returns A promise that resolves to the query result
   */
  executeRaw(sql: string, params?: unknown[]): Promise<unknown>;

  /**
   * Starts a transaction.
   * 
   * @returns A promise that resolves to a transaction object
   */
  startTransaction(): Promise<Transaction>;
}

/**
 * Represents a database transaction.
 */
export interface Transaction {
  /**
   * Commits the transaction.
   * 
   * @returns A promise that resolves when the transaction is committed
   */
  commit(): Promise<void>;

  /**
   * Rolls back the transaction.
   * 
   * @returns A promise that resolves when the transaction is rolled back
   */
  rollback(): Promise<void>;
} 