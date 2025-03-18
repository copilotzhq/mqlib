/**
 * PostgreSQL Connection Factory for MQLib
 * 
 * This module provides a factory function to create PostgreSQL connections
 * that are compatible with the MQLib library.
 */

/**
 * Configuration options for PostgreSQL connection.
 */
export interface PostgresConnectionOptions {
  /** The hostname of the PostgreSQL server */
  hostname: string;
  /** The port of the PostgreSQL server */
  port?: number;
  /** The username for authentication */
  username: string;
  /** The password for authentication */
  password: string;
  /** The database name */
  database: string;
  /** Whether to use SSL */
  ssl?: boolean;
}

/**
 * Creates a PostgreSQL connection that can be used with MQLib.
 * 
 * @param options The connection options
 * @returns A connection object that can be used with MQLib
 */
export async function createPostgresConnection(options: PostgresConnectionOptions) {
  // Import the PostgreSQL module dynamically to avoid dependency issues
  const { Client } = await import("https://deno.land/x/postgres@v0.17.0/mod.ts");
  
  // Create a new PostgreSQL client
  const client = new Client({
    hostname: options.hostname,
    port: options.port || 5432,
    user: options.username,
    password: options.password,
    database: options.database,
    tls: options.ssl ? { enabled: true } : undefined
  });
  
  // Connect to the database
  await client.connect();
  
  return {
    /**
     * Executes a SQL query with parameters.
     * 
     * @param sql The SQL query to execute
     * @param params The parameters for the query
     * @returns The raw query result for the adapter to process
     */
    query: async (sql: string, params: unknown[] = []) => {
      try {
        // Return the raw result for the adapter to process
        return await client.queryObject(sql, params);
      } catch (error) {
        console.error("PostgreSQL Error:", error);
        throw error;
      }
    },
    
    /**
     * Closes the database connection.
     */
    close: async () => {
      await client.end();
    }
  };
} 