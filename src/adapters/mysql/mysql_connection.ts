/**
 * MySQL Connection Factory for MQLib
 * 
 * This module provides a factory function to create MySQL connections
 * that are compatible with the MQLib library.
 */

/**
 * Configuration options for MySQL connection.
 */
export interface MySqlConnectionOptions {
  /** The hostname of the MySQL server */
  hostname: string;
  /** The port of the MySQL server */
  port?: number;
  /** The username for authentication */
  username: string;
  /** The password for authentication */
  password: string;
  /** The database name */
  database: string;
  /** Whether to use SSL */
  ssl?: boolean;
  /** Connection pool size */
  poolSize?: number;
}

/**
 * Creates a MySQL connection that can be used with MQLib.
 * 
 * @param options The connection options
 * @returns A connection object that can be used with MQLib
 */
export async function createMySqlConnection(options: MySqlConnectionOptions) {
  // Import the MySQL module dynamically to avoid dependency issues
  const { Client } = await import("https://deno.land/x/mysql@v2.11.0/mod.ts");
  
  // Create a new MySQL client
  const client = await new Client().connect({
    hostname: options.hostname,
    port: options.port || 3306,
    username: options.username,
    password: options.password,
    db: options.database,
    poolSize: options.poolSize || 10,
    // SSL options would be configured here if needed
  });
  
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
        const result = await client.execute(sql, params);
        
        // Return the raw result for the adapter to process
        return result;
      } catch (error) {
        console.error("MySQL Error:", error);
        throw error;
      }
    },
    
    /**
     * Closes the database connection.
     */
    close: async () => {
      await client.close();
    }
  };
} 