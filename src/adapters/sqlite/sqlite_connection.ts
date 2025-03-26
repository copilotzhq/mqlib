/**
 * SQLite Connection Factory for MQLib
 * 
 * This module provides a factory function to create SQLite connections
 * that are compatible with the MQLib library through dependency injection,
 * supporting both WASM-based and native FFI-based SQLite implementations.
 */

/**
 * Interface for the normalized SQLite connection used internally
 */
interface NormalizedConnection {
  query: (sql: string, params: any[]) => any;
  close: () => void;
  type: "wasm" | "native";
}

/**
 * Library type detection result
 */
type SqliteLibraryType = "wasm" | "native" | "unknown";

/**
 * Interface for the standardized SQLite connection returned to users
 */
export interface StandardizedSqliteConnection {
  /**
   * Executes a SQL query with parameters.
   */
  query: (sql: string, params?: unknown[]) => Promise<any>;
  
  /**
   * Begins a new transaction.
   */
  beginTransaction: () => Promise<void>;

  /**
   * Commits the current transaction.
   */
  commitTransaction: () => Promise<void>;

  /**
   * Rolls back the current transaction.
   */
  rollbackTransaction: () => Promise<void>;
  
  /**
   * Closes the database connection.
   */
  close: () => void;

  /**
   * Returns the type of SQLite implementation being used.
   */
  getImplementationType: () => "wasm" | "native";
}

/**
 * Creates a connection using an injected SQLite library instance
 * 
 * @param library The SQLite library instance
 * @param dbPath The path to the SQLite database file, or ":memory:" for an in-memory database
 * @returns A connection object that can be used with MQLib
 */
export async function createConnection(
  library: any, 
  dbPath: string = ":memory:"
): Promise<StandardizedSqliteConnection> {
  if (!library) {
    throw new Error("SQLite library instance is required");
  }

  const libraryType = detectLibraryType(library);
  let connection: NormalizedConnection;

  if (libraryType === "native") {
    connection = createNativeConnectionFromLibrary(library, dbPath);
  } else if (libraryType === "wasm") {
    connection = createWasmConnectionFromLibrary(library, dbPath);
  } else {
    throw new Error("Unsupported SQLite library type. Please provide a compatible library.");
  }

  return createStandardizedConnection(connection);
}

/**
 * Detects the type of SQLite library provided
 * 
 * @param library The SQLite library instance
 * @returns The type of the library: "wasm", "native", or "unknown"
 */
function detectLibraryType(library: any): SqliteLibraryType {
  // Check for native SQLite3 (Database class with prepare method)
  if (library.Database && typeof library.Database === 'function') {
    return "native";
  }
  
  // Check for WASM SQLite (DB class with query method)
  if (library.DB && typeof library.DB === 'function') {
    return "wasm";
  }

  // If we already have an instantiated Database/DB object
  if (typeof library.prepare === 'function') {
    return "native";
  }
  
  if (typeof library.query === 'function') {
    return "wasm";
  }

  return "unknown";
}

/**
 * Creates a standardized connection interface from a normalized connection
 */
function createStandardizedConnection(connection: NormalizedConnection): StandardizedSqliteConnection {
  return {
    /**
     * Executes a SQL query with parameters.
     * 
     * @param sql The SQL query to execute
     * @param params The parameters for the query
     * @returns The raw query result for the adapter to process
     */
    query: async (sql: string, params: unknown[] = []) => {
      return await connection.query(sql, params);
    },
    
    /**
     * Begins a new transaction.
     */
    beginTransaction: async () => {
      await connection.query("BEGIN TRANSACTION", []);
    },

    /**
     * Commits the current transaction.
     */
    commitTransaction: async () => {
      await connection.query("COMMIT", []);
    },

    /**
     * Rolls back the current transaction.
     */
    rollbackTransaction: async () => {
      await connection.query("ROLLBACK", []);
    },
    
    /**
     * Closes the database connection.
     */
    close: () => {
      connection.close();
    },

    /**
     * Returns the type of SQLite implementation being used.
     */
    getImplementationType: () => {
      return connection.type;
    }
  };
}

/**
 * Creates a native connection using a provided library instance
 */
function createNativeConnectionFromLibrary(libraryInstance: any, dbPath: string): NormalizedConnection {
  try {
    // Determine if we have a Database class or an instance
    const Database = libraryInstance.Database || libraryInstance.constructor;
    const db = libraryInstance.prepare ? libraryInstance : new Database(dbPath);
    
    // Enable JSON1 extension for handling nested objects and arrays
    try {
      db.prepare("SELECT json_extract('{}', '$')").get();
    } catch (error) {
      console.warn("SQLite JSON1 extension not available. Nested objects and arrays may not work correctly.");
    }
    
    return {
      query: (sql: string, params: any[] = []) => {
        try {
          // Prepare the statement
          const stmt = db.prepare(sql);
          
          if (sql.trim().toUpperCase().startsWith("SELECT")) {
            // For SELECT queries, return rows and rowCount
            if (sql.includes("COUNT(")) {
              // Handle COUNT queries specially - get() returns a single row as an object
              const result = stmt.get(...params);
              // Extract the count value from the first property of the result object
              const count = result ? Object.values(result)[0] as number : 0;
              return { rows: [{ count }], rowCount: 1 };
            } else {
              // For other SELECT queries - all() returns all rows
              const rows = stmt.all(...params);
              
              // Deep process JSON fields in each row - critical for complex objects
              const processedRows = rows.map((row: Record<string, any>) => {
                const processed = parseJsonFields(row);
                
                // Additional processing for array properties that might be string JSON
                for (const [key, value] of Object.entries(processed)) {
                  if (key === 'tags' || key === 'skills' || key === 'favorites') {
                    if (typeof value === 'string' && (value.startsWith('[') || value.startsWith('{'))) {
                      try {
                        processed[key] = JSON.parse(value);
                      } catch (e) {
                        // Keep as is if parsing fails
                      }
                    }
                  } else if (key.endsWith('.name') && typeof value === 'string') {
                    // Special handling for nested properties that may be accessed directly
                    const parts = key.split('.');
                    if (parts.length > 1) {
                      const parentKey = parts[0];
                      if (parentKey in processed && processed[parentKey] === null) {
                        processed[parentKey] = {};
                      }
                    }
                  }
                }
                
                return processed;
              });
              
              // Extract column names from first row if available
              const columnNames: string[] = rows.length > 0 ? Object.keys(rows[0] || {}) : [];
              
              return { 
                rawResult: processedRows, 
                rowCount: processedRows.length, 
                columnNames 
              };
            }
          } else if (sql.trim().toUpperCase().startsWith("CREATE TABLE")) {
            // Execute the CREATE TABLE statement
            stmt.run(...params);
            return { rowCount: 0 };
          } else if (sql.trim().toUpperCase().startsWith("INSERT")) {
            // Execute the INSERT statement and return the last inserted ID
            stmt.run(...params);
            // Access lastInsertRowId and changes from the database
            return { 
              rowCount: db.changes, 
              lastInsertId: db.lastInsertRowId 
            };
          } else {
            // For other queries (UPDATE, DELETE, etc.)
            stmt.run(...params);
            return { rowCount: db.changes };
          }
        } catch (error) {
          console.error("SQLite Error:", error);
          throw error;
        }
      },
      close: () => {
        try {
          db.close();
        } catch (error) {
          console.warn("Error closing native SQLite connection:", error);
        }
      },
      type: "native"
    };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to create native SQLite connection: ${errorMessage}`);
  }
}

/**
 * Creates a WASM connection using a provided library instance
 */
function createWasmConnectionFromLibrary(libraryInstance: any, dbPath: string): NormalizedConnection {
  try {
    // Determine if we have a DB class or an instance
    const DB = libraryInstance.DB || libraryInstance.constructor;
    const db = libraryInstance.query ? libraryInstance : new DB(dbPath);
    
    // Enable JSON1 extension for handling nested objects and arrays
    try {
      db.query("SELECT json_extract('{}', '$')");
    } catch (error) {
      console.warn("SQLite JSON1 extension not available. Nested objects and arrays may not work correctly.");
    }
    
    return {
      query: (sql: string, params: any[] = []) => {
        try {
          // Type cast params to any[] to satisfy SQLite types
          const safeParams = params as any[];
          
          if (sql.trim().toUpperCase().startsWith("SELECT")) {
            // For SELECT queries, return the raw result
            if (sql.includes("COUNT(")) {
              // Handle COUNT queries specially - removing the generic type
              const result = db.query(sql, safeParams);
              const rows = [];
              for (const [count] of result) {
                rows.push({ count });
              }
              return { rows, rowCount: rows.length };
            } else {
              // For other SELECT queries, return the raw result with column names
              const result = db.query(sql, safeParams);
              
              // Get column names from the query
              // This is a bit of a hack, but SQLite doesn't provide column names directly
              // We'll execute a dummy query to get the column names
              let columnNames: string[] = [];
              try {
                // Extract table name from the query
                const tableNameMatch = sql.match(/FROM\s+["']?([a-zA-Z0-9_]+)["']?/i);
                if (tableNameMatch && tableNameMatch[1]) {
                  const tableName = tableNameMatch[1];
                  const tableInfo = db.query(`PRAGMA table_info(${tableName})`);
                  columnNames = tableInfo.map((row: any) => row[1] as string);
                }
              } catch (e) {
                console.warn("Failed to get column names:", e);
              }
              
              return { rawResult: result, rowCount: result.length, columnNames };
            }
          } else if (sql.trim().toUpperCase().startsWith("CREATE TABLE")) {
            // Execute the CREATE TABLE statement
            db.query(sql, safeParams);
            return { rowCount: 0 };
          } else if (sql.trim().toUpperCase().startsWith("INSERT")) {
            // Execute the INSERT statement and return the last inserted ID
            db.query(sql, safeParams);
            return { rowCount: db.changes, lastInsertId: db.lastInsertRowId };
          } else {
            // For other queries (UPDATE, DELETE, etc.), execute and return rowCount
            db.query(sql, safeParams);
            return { rowCount: db.changes };
          }
        } catch (error) {
          console.error("SQLite Error:", error);
          throw error;
        }
      },
      close: () => {
        try {
          db.close();
        } catch (error) {
          console.warn("Error closing WASM SQLite connection:", error);
        }
      },
      type: "wasm"
    };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to create WASM SQLite connection: ${errorMessage}`);
  }
}

/**
 * Helper function to parse JSON strings in result objects
 */
function parseJsonFields(obj: Record<string, any>): Record<string, any> {
  const result: Record<string, any> = {};
  
  for (const [key, value] of Object.entries(obj)) {
    if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
      try {
        result[key] = JSON.parse(value);
      } catch (e) {
        result[key] = value;
      }
    } else if (typeof value === 'object' && value !== null) {
      // Recursively parse object properties
      result[key] = parseJsonFields(value);
    } else {
      result[key] = value;
    }
  }
  
  return result;
} 