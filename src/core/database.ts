/**
 * Database implementation for MQLib
 */

import { Database as IDatabase } from "../interfaces/database.ts";
import { Collection } from "./collection.ts";
import { Document } from "../types/document.ts";
import { SqlAdapter } from "../interfaces/adapter.ts";

/**
 * Implementation of the Database interface.
 * 
 * This class provides access to collections in a SQL database.
 */
export class Database implements IDatabase {
  /**
   * Gets the name of the database.
   */
  readonly name: string;

  /**
   * The SQL adapter to use for translating operations.
   */
  private adapter: SqlAdapter;

  /**
   * The database connection to use for executing queries.
   */
  private connection: any; // This will be replaced with a proper connection type

  /**
   * Cache of collection instances.
   */
  private collections: Map<string, Collection<any>> = new Map();

  /**
   * Creates a new Database instance.
   * 
   * @param name The name of the database
   * @param adapter The SQL adapter to use
   * @param connection The database connection to use
   */
  constructor(name: string, adapter: SqlAdapter, connection: any) {
    this.name = name;
    this.adapter = adapter;
    this.connection = connection;
  }

  /**
   * Gets a collection with the specified name.
   * 
   * @template T The document type stored in the collection
   * @param name The name of the collection
   * @returns A Collection instance for the specified name
   */
  collection<T extends Document>(name: string): Collection<T> {
    if (!this.collections.has(name)) {
      this.collections.set(name, new Collection<T>(name, this.adapter, this.connection));
    }
    
    return this.collections.get(name) as Collection<T>;
  }

  /**
   * Lists all collections in the database.
   * 
   * @returns A promise that resolves to an array of collection names
   */
  async listCollections(): Promise<string[]> {
    // This implementation is database-specific and will need to be customized
    // for each SQL dialect
    
    // For SQLite, we can query the sqlite_master table
    if (this.adapter.dialect === "sqlite") {
      const sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
      
      try {
        const result = await this.connection.query(sql);
        
        if (result.rows && result.rows.length > 0) {
          return result.rows.map((row: any) => row.name);
        }
        
        return [];
      } catch (error) {
        console.error("Error in listCollections:", error);
        throw error;
      }
    }
    
    // For PostgreSQL, we can query the information_schema
    if (this.adapter.dialect === "postgres") {
      const sql = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
      `;
      
      try {
        const result = await this.connection.query(sql);
        
        if (result.rows && result.rows.length > 0) {
          return result.rows.map((row: any) => row.table_name);
        }
        
        return [];
      } catch (error) {
        console.error("Error in listCollections:", error);
        throw error;
      }
    }
    
    // For MySQL, we can use SHOW TABLES
    if (this.adapter.dialect === "mysql") {
      const sql = "SHOW TABLES";
      
      try {
        const result = await this.connection.query(sql);
        
        if (result.rows && result.rows.length > 0) {
          return result.rows.map((row: any) => Object.values(row)[0]);
        }
        
        return [];
      } catch (error) {
        console.error("Error in listCollections:", error);
        throw error;
      }
    }
    
    throw new Error(`Unsupported SQL dialect: ${this.adapter.dialect}`);
  }

  /**
   * Creates a collection with the specified name and schema.
   * 
   * @param name The name of the collection to create
   * @param schema The JSON schema for the collection (optional)
   * @returns A promise that resolves to the created collection
   */
  async createCollection<T extends Document>(name: string, schema?: object): Promise<Collection<T>> {
    if (!schema) {
      // Create a minimal schema with just an _id field
      schema = {
        type: "object",
        properties: {
          _id: { type: "string" }
        },
        required: ["_id"]
      };
    }
    
    // Translate the schema to a SQL CREATE TABLE statement
    const createTableQuery = this.adapter.translateSchema(name, schema);
    
    // Execute the query
    try {
      await this.connection.query(createTableQuery.sql, createTableQuery.params);
      
      // Return a collection instance for the new table
      return this.collection<T>(name);
    } catch (error) {
      console.error("Error in createCollection:", error);
      throw error;
    }
  }

  /**
   * Drops a collection from the database.
   * 
   * @param name The name of the collection to drop
   * @returns A promise that resolves when the collection is dropped
   */
  async dropCollection(name: string): Promise<void> {
    // Build the SQL query
    const sql = `DROP TABLE IF EXISTS ${this.adapter.escapeIdentifier(name)}`;
    
    // Execute the query
    try {
      await this.connection.query(sql);
      
      // Remove the collection from the cache
      this.collections.delete(name);
    } catch (error) {
      console.error("Error in dropCollection:", error);
      throw error;
    }
  }
} 