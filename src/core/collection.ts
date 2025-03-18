/**
 * Collection implementation for MQLib
 */

import type { Collection as ICollection, IndexOptions } from "../interfaces/collection.ts";
import type { SqlAdapter } from "../interfaces/adapter.ts";
import type { Document, WithId, OptionalId, DocumentId } from "../types/document.ts";
import type { Filter, FindOptions } from "../types/filter.ts";
import type { UpdateOperator, UpdateOptions, DeleteOptions } from "../types/update.ts";
import { ObjectId } from "bson";
import type { 
  InsertOneResult, 
  InsertManyResult, 
  UpdateResult, 
  DeleteResult,
  IndexInfo
} from "../types/results.ts";

/**
 * Implementation of the Collection interface.
 * 
 * This class provides MongoDB-like operations for a SQL table.
 * 
 * @template T The document type stored in this collection
 */
export class Collection<T extends Document> implements ICollection<T> {
  /**
   * Gets the name of the collection.
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
   * Creates a new Collection instance.
   * 
   * @param name The name of the collection
   * @param adapter The SQL adapter to use
   * @param connection The database connection to use
   */
  constructor(name: string, adapter: SqlAdapter, connection: any) {
    this.name = name;
    this.adapter = adapter;
    this.connection = connection;
  }

  /**
   * Finds a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to the matching document, or null if none is found
   */
  async findOne(filter: Filter<T>, options: FindOptions<T> = {}): Promise<WithId<T> | null> {
    // Limit to 1 document
    const findOptions = { ...options, limit: 1 };
    
    // Translate the filter to a SQL WHERE clause
    const whereClause = this.adapter.translateFilter(filter);
    
    // Build the SQL query
    const sql = `SELECT * FROM ${this.adapter.escapeIdentifier(this.name)} ${whereClause.sql} LIMIT 1`;
    
    // Execute the query
    try {
      const result = await this.connection.query(sql, whereClause.params);
      const processedResult = this.adapter.processQueryResult(result, this.name);
      
      if (processedResult.rows.length === 0) {
        return null;
      }
      
      return this.rowToDocument(processedResult.rows[0]);
    } catch (error) {
      console.error("Error in findOne:", error);
      throw error;
    }
  }

  /**
   * Finds all documents that match the filter.
   * 
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to an array of matching documents
   */
  async find(filter: Filter<T>, options: FindOptions<T> = {}): Promise<WithId<T>[]> {
    // Use the adapter to translate the filter and options into a SQL query
    // All dialects should implement a translateFind method 
    // If an adapter doesn't have it, the base implementation will be used
    let query;
    
    if ('translateFind' in this.adapter) {
      // Use the adapter's translateFind method
      query = (this.adapter as any).translateFind(this.name, filter, options);
    } else {
      // Fallback to manual query building using the base adapter methods
      const whereClause = this.adapter.translateFilter(filter);
      
      let sql = `SELECT * FROM ${this.adapter.escapeIdentifier(this.name)} ${whereClause.sql}`;
      const params = [...whereClause.params];
      
      // Add sorting
      if (options.sort) {
        const sortClauses: string[] = [];
        
        for (const [field, direction] of Object.entries(options.sort)) {
          sortClauses.push(`${this.adapter.escapeIdentifier(field)} ${direction === 1 ? 'ASC' : 'DESC'}`);
        }
        
        if (sortClauses.length > 0) {
          sql += ` ORDER BY ${sortClauses.join(', ')}`;
        }
      }
      
      // Add pagination
      if (options.limit !== undefined) {
        sql += ` LIMIT ?`;
        params.push(options.limit);
      } else if (options.skip !== undefined) {
        // Add a very large LIMIT if needed for offset compatibility
        sql += ` LIMIT 9223372036854775807`;
      }
      
      if (options.skip !== undefined) {
        sql += ` OFFSET ?`;
        params.push(options.skip);
      }
      
      query = { sql, params };
    }
    
    
    // Execute the query
    try {
      const result = await this.connection.query(query.sql, query.params);
      const processedResult = this.adapter.processQueryResult(result, this.name);
      
      // Apply projection if specified
      if (options.projection) {
        return processedResult.rows.map(row => this.applyProjection(this.rowToDocument(row), options.projection));
      }
      
      return processedResult.rows.map(row => this.rowToDocument(row));
    } catch (error) {
      console.error("Error in find:", error);
      throw error;
    }
  }

  /**
   * Inserts a single document into the collection.
   * 
   * @param doc The document to insert
   * @returns A promise that resolves to an InsertOneResult
   */
  async insertOne(doc: OptionalId<T>): Promise<InsertOneResult> {
    // Generate an ID if not provided
    const _id = doc._id || this.generateId();
    const docToInsert = { ...doc, _id } as T;
    
    // Translate the document to a SQL INSERT statement
    const insertQuery = this.adapter.translateInsert(this.name, docToInsert);
    
    // Execute the query
    try {
      await this.connection.query(insertQuery.sql, insertQuery.params);
      
      return {
        acknowledged: true,
        insertedId: _id
      };
    } catch (error) {
      console.error("Error in insertOne:", error);
      throw error;
    }
  }

  /**
   * Inserts multiple documents into the collection.
   * 
   * @param docs The documents to insert
   * @returns A promise that resolves to an InsertManyResult
   */
  async insertMany(docs: OptionalId<T>[]): Promise<InsertManyResult> {
    if (!Array.isArray(docs) || docs.length === 0) {
      throw new Error("docs parameter must be a non-empty array");
    }
    
    const insertedIds: DocumentId[] = [];
    const writeErrors: { index: number; error: Error }[] = [];
    
    // Insert each document individually
    // In a real implementation, we would use a transaction and batch inserts
    for (let i = 0; i < docs.length; i++) {
      try {
        const result = await this.insertOne(docs[i]);
        insertedIds.push(result.insertedId);
      } catch (error) {
        writeErrors.push({
          index: i,
          error: error instanceof Error ? error : new Error(String(error))
        });
      }
    }
    
    return {
      acknowledged: true,
      insertedCount: insertedIds.length,
      insertedIds,
      hasWriteErrors: writeErrors.length > 0,
      writeErrors: writeErrors.length > 0 ? writeErrors : undefined
    };
  }

  /**
   * Updates a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  async updateOne(filter: Filter<T>, update: UpdateOperator<T>, options: UpdateOptions = {}): Promise<UpdateResult<T>> {
    const { upsert = false } = options;
    
    // Check if a document exists
    const existingDoc = await this.findOne(filter);
    
    if (!existingDoc && upsert) {
      // If no document exists and upsert is true, insert a new document
      const newDoc: Record<string, unknown> = {};
      
      // Apply $setOnInsert values
      if (update.$setOnInsert) {
        Object.assign(newDoc, update.$setOnInsert);
      }
      
      // Apply $set values
      if (update.$set) {
        Object.assign(newDoc, update.$set);
      }
      
      // Apply filter values for equality conditions
      for (const [key, value] of Object.entries(filter)) {
        if (typeof value !== 'object' || value === null) {
          newDoc[key] = value;
        }
      }
      
      const result = await this.insertOne(newDoc as OptionalId<T>);
      
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 1,
        upsertedId: result.insertedId
      };
    }
    
    if (!existingDoc) {
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
        upsertedId: null
      };
    }
    
    // For nested fields, we'll handle them separately
    let matchedCount = 0;
    let modifiedCount = 0;
    
    // Handle $set operations
    if (update.$set) {
      for (const [field, value] of Object.entries(update.$set)) {
        // Create a new update with just this field
        const singleFieldUpdate: UpdateOperator<T> = {
          $set: { [field]: value } as any
        };
        
        // Translate the update to a SQL UPDATE statement
        const updateQuery = this.adapter.translateUpdate(this.name, singleFieldUpdate, filter);
        
        // Skip empty SQL queries (adapter determined there's nothing to update)
        if (!updateQuery.sql) {
          continue;
        }
        
        // Execute the query
        try {
          const result = await this.connection.query(updateQuery.sql, updateQuery.params);
          
          if (result.rowCount > 0) {
            matchedCount = 1;
            modifiedCount = 1;
          }
        } catch (error) {
          console.error(`Error in updateOne field ${field}:`, error);
          throw error;
        }
      }
    }
    
    // Handle $inc operations separately
    if (update.$inc) {
      // Create a new update with just $inc operations
      const incUpdate: UpdateOperator<T> = {
        $inc: update.$inc
      };
      
      // Translate the update to a SQL UPDATE statement
      const updateQuery = this.adapter.translateUpdate(this.name, incUpdate, filter);
      
      // Skip empty SQL queries
      if (updateQuery.sql) {
        
        // Execute the query
        try {
          const result = await this.connection.query(updateQuery.sql, updateQuery.params);
          
          if (result.rowCount > 0) {
            matchedCount = 1;
            modifiedCount = 1;
          }
        } catch (error) {
          console.error("Error in updateOne $inc operations:", error);
          throw error;
        }
      }
    }
    
    // Handle other update operations
    const otherOperations: UpdateOperator<T> = {};
    if (update.$push) otherOperations.$push = update.$push;
    if (update.$pull) otherOperations.$pull = update.$pull;
    if (update.$addToSet) otherOperations.$addToSet = update.$addToSet;
    if (update.$unset) otherOperations.$unset = update.$unset;
    
    if (Object.keys(otherOperations).length > 0) {
      // Translate the update to a SQL UPDATE statement
      const updateQuery = this.adapter.translateUpdate(this.name, otherOperations, filter);
      
      // Skip empty SQL queries (adapter determined there's nothing to update)
      if (updateQuery.sql) {
        
        // Execute the query
        try {
          const result = await this.connection.query(updateQuery.sql, updateQuery.params);
          
          if (result.rowCount > 0) {
            matchedCount = 1;
            modifiedCount = 1;
          }
        } catch (error) {
          console.error("Error in updateOne other operations:", error);
          throw error;
        }
      }
    }
    
    return {
      acknowledged: true,
      matchedCount,
      modifiedCount,
      upsertedCount: 0,
      upsertedId: null
    };
  }

  /**
   * Updates all documents that match the filter.
   * 
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  async updateMany(filter: Filter<T>, update: UpdateOperator<T>, options: UpdateOptions = {}): Promise<UpdateResult<T>> {
    const { upsert = false } = options;
    
    // Check if any documents exist
    const count = await this.countDocuments(filter);
    
    if (count === 0 && upsert) {
      // If no documents exist and upsert is true, insert a new document
      return this.updateOne(filter, update, options);
    }
    
    if (count === 0) {
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
        upsertedId: null
      };
    }
    
    // Translate the update to a SQL UPDATE statement
    const updateQuery = this.adapter.translateUpdate(this.name, update, filter);
    
    // Execute the query
    try {
      const result = await this.connection.query(updateQuery.sql, updateQuery.params);
      
      return {
        acknowledged: true,
        matchedCount: count,
        modifiedCount: result.rowCount,
        upsertedCount: 0,
        upsertedId: null
      };
    } catch (error) {
      console.error("Error in updateMany:", error);
      throw error;
    }
  }

  /**
   * Deletes a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param options Options for the delete operation
   * @returns A promise that resolves to a DeleteResult
   */
  async deleteOne(filter: Filter<T>, options: DeleteOptions = {}): Promise<DeleteResult> {
    // Find the document first to make sure it exists
    const doc = await this.findOne(filter);
    
    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0
      };
    }
    
    // Translate the filter to a SQL DELETE statement
    const deleteQuery = this.adapter.translateDelete(this.name, { _id: doc._id } as Filter<T>);
    
    // Execute the query
    try {
      await this.connection.query(deleteQuery.sql, deleteQuery.params);
      
      return {
        acknowledged: true,
        deletedCount: 1
      };
    } catch (error) {
      console.error("Error in deleteOne:", error);
      throw error;
    }
  }

  /**
   * Deletes all documents that match the filter.
   * 
   * @param filter The query filter
   * @param options Options for the delete operation
   * @returns A promise that resolves to a DeleteResult
   */
  async deleteMany(filter: Filter<T>, options: DeleteOptions = {}): Promise<DeleteResult> {
    // Translate the filter to a SQL DELETE statement
    const deleteQuery = this.adapter.translateDelete(this.name, filter);
    
    // Execute the query
    try {
      const result = await this.connection.query(deleteQuery.sql, deleteQuery.params);
      
      return {
        acknowledged: true,
        deletedCount: result.rowCount
      };
    } catch (error) {
      console.error("Error in deleteMany:", error);
      throw error;
    }
  }

  /**
   * Counts the number of documents that match the filter.
   * 
   * @param filter The query filter
   * @returns A promise that resolves to the count
   */
  async countDocuments(filter: Filter<T> = {}): Promise<number> {
    // Translate the filter to a SQL WHERE clause
    const whereQuery = this.adapter.translateFilter(filter);
    
    // Build the SQL query
    const sql = `SELECT COUNT(*) as count FROM ${this.adapter.escapeIdentifier(this.name)} ${whereQuery.sql}`;
    
    // Execute the query
    try {
      const result = await this.connection.query(sql, whereQuery.params);
      
      if (result.rows && result.rows.length > 0) {
        return parseInt(result.rows[0].count, 10);
      }
      
      return 0;
    } catch (error) {
      console.error("Error in countDocuments:", error);
      throw error;
    }
  }

  /**
   * Creates an index on the specified field(s).
   * 
   * @param fieldOrSpec The field name or index specification
   * @param options Options for the index
   * @returns A promise that resolves to the name of the created index
   */
  async createIndex(fieldOrSpec: string | Record<string, 1 | -1>, options: IndexOptions = {}): Promise<string> {
    // Normalize the field specification
    const fields: Record<string, 1 | -1> = typeof fieldOrSpec === 'string' 
      ? { [fieldOrSpec]: 1 as 1 } 
      : fieldOrSpec;
    
    // Generate an index name if not provided
    const indexName = options.name || this.generateIndexName(fields);
    
    // Translate the index specification to a SQL CREATE INDEX statement
    const indexQuery = this.adapter.translateCreateIndex(
      this.name,
      indexName,
      fields,
      { unique: options.unique, sparse: options.sparse }
    );
    
    // Execute the query
    try {
      await this.connection.query(indexQuery.sql, indexQuery.params);
      
      return indexName;
    } catch (error) {
      console.error("Error in createIndex:", error);
      throw error;
    }
  }

  /**
   * Lists all indexes on the collection.
   * 
   * @returns A promise that resolves to an array of index information
   */
  async listIndexes(): Promise<IndexInfo[]> {
    // This implementation is database-specific
    if (this.adapter.dialect === "sqlite") {
      // For SQLite, query the sqlite_master table for indexes
      const sql = `SELECT name, sql FROM sqlite_master 
                  WHERE type = 'index' 
                  AND tbl_name = ? 
                  AND name NOT LIKE 'sqlite_autoindex%'`;
      
      try {
        const result = await this.connection.query(sql, [this.name]);
        
        if (result.rows && result.rows.length > 0) {
          return result.rows.map((row: any) => {
            // Extract index information from the SQL statement
            const name = row.name;
            const sql = row.sql || "";
            const unique = sql.toLowerCase().includes("unique");
            
            // For SQLite, we can extract the field name from the index name
            // Example: users_email_asc -> email
            const indexNameParts = name.split('_');
            if (indexNameParts.length >= 3) {
              // Remove the table name prefix and the direction suffix
              const fieldName = indexNameParts[1];
              // Determine direction from suffix (asc or desc)
              const direction = indexNameParts[2] === 'desc' ? -1 : 1;
              
              const key: Record<string, 1 | -1> = {};
              key[fieldName] = direction;
              
              return {
                name,
                key,
                unique
              };
            }
            
            // Fallback to parsing the SQL if the index name doesn't follow the convention
            const fieldMatch = /ON\s+(?:"|`)?[^"`]+(?:"|`)?\s+\(([^)]+)\)/i.exec(sql);
            const fieldStr = fieldMatch ? fieldMatch[1] : "";
            const fields = fieldStr.split(",").map(f => f.trim());
            
            const key: Record<string, 1 | -1> = {};
            fields.forEach(field => {
              // Remove quotes from field name
              const cleanField = field.replace(/["'`]/g, "");
              // Default to ascending order if not specified
              const direction = cleanField.toLowerCase().includes(" desc") ? -1 : 1;
              // Remove direction specifier from field name
              const fieldName = cleanField.replace(/ (asc|desc)$/i, "");
              key[fieldName] = direction;
            });
            
            return {
              name,
              key,
              unique
            };
          });
        }
        
        return [];
      } catch (error) {
        console.error("Error in listIndexes:", error);
        throw error;
      }
    } else if (this.adapter.dialect === "postgres") {
      // For PostgreSQL, query the pg_indexes view
      const sql = `
        SELECT
          i.relname AS name,
          am.amname AS method,
          ix.indisunique AS unique,
          array_to_string(array_agg(a.attname), ',') AS columns,
          array_to_string(array_agg(
            CASE WHEN ix.indoption[a.attnum-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END
          ), ',') AS directions
        FROM
          pg_index ix
          JOIN pg_class i ON i.oid = ix.indexrelid
          JOIN pg_class t ON t.oid = ix.indrelid
          JOIN pg_am am ON i.relam = am.oid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE
          t.relname = $1
          AND n.nspname = 'public'
        GROUP BY
          i.relname, am.amname, ix.indisunique
        ORDER BY
          i.relname;
      `;
      
      try {
        const result = await this.connection.query(sql, [this.name]);
        
        if (result.rows && result.rows.length > 0) {
          return result.rows.map((row: any) => {
            const name = row.name;
            const unique = row.unique;
            const columns = (row.columns || "").split(",");
            const directions = (row.directions || "").split(",");
            
            const key: Record<string, 1 | -1> = {};
            for (let i = 0; i < columns.length; i++) {
              const direction = directions[i] === "DESC" ? -1 : 1;
              key[columns[i]] = direction;
            }
            
            return {
              name,
              key,
              unique
            };
          });
        }
        
        return [];
      } catch (error) {
        console.error("Error in listIndexes for PostgreSQL:", error);
        throw error;
      }
    } else if (this.adapter.dialect === "mysql") {
      // For MySQL, query the information_schema.statistics table
      const sql = `
        SELECT
          index_name AS name,
          column_name,
          non_unique,
          seq_in_index,
          CASE WHEN collation = 'D' THEN 'DESC' ELSE 'ASC' END AS direction
        FROM
          information_schema.statistics
        WHERE
          table_schema = DATABASE()
          AND table_name = ?
        ORDER BY
          index_name, seq_in_index
      `;
      
      try {
        const result = await this.connection.query(sql, [this.name]);
        
        if (result.rows && result.rows.length > 0) {
          // Group by index name
          const indexMap = new Map<string, { 
            name: string; 
            key: Record<string, 1 | -1>; 
            unique: boolean;
          }>();
          
          for (const row of result.rows) {
            const name = row.name;
            const column = row.column_name;
            const direction = row.direction === "DESC" ? -1 : 1;
            const unique = !row.non_unique;
            
            if (!indexMap.has(name)) {
              indexMap.set(name, {
                name,
                key: {},
                unique
              });
            }
            
            const index = indexMap.get(name)!;
            index.key[column] = direction;
          }
          
          return Array.from(indexMap.values());
        }
        
        return [];
      } catch (error) {
        console.error("Error in listIndexes for MySQL:", error);
        throw error;
      }
    }
    
    // For other databases, we'll need to implement specific queries
    throw new Error(`listIndexes not implemented for ${this.adapter.dialect}`);
  }

  /**
   * Drops an index from the collection.
   * 
   * @param indexName The name of the index to drop
   * @returns A promise that resolves when the index is dropped
   */
  async dropIndex(indexName: string): Promise<void> {
    // Build the SQL query
    const sql = `DROP INDEX IF EXISTS ${this.adapter.escapeIdentifier(indexName)}`;
    
    // Execute the query
    try {
      await this.connection.query(sql);
    } catch (error) {
      console.error("Error in dropIndex:", error);
      throw error;
    }
  }

  /**
   * Generates a unique ID for a document.
   * 
   * @returns A unique ID
   */
  private generateId(): DocumentId {
    return new ObjectId().toString();
  }

  /**
   * Generates a name for an index based on the fields it covers.
   * 
   * @param fields The fields in the index
   * @returns The generated index name
   */
  private generateIndexName(fields: Record<string, 1 | -1>): string {
    const parts = Object.entries(fields).map(([field, direction]) => 
      `${field}_${direction === 1 ? 'asc' : 'desc'}`
    );
    
    return `${this.name}_${parts.join('_')}`;
  }

  /**
   * Converts a SQL row to a document.
   * 
   * @param row The SQL row
   * @returns The document
   */
  private rowToDocument(row: Record<string, unknown>): WithId<T> {
    // Check if this is the old format with a 'data' column
    if ('data' in row && typeof row.data === 'string') {
      try {
        // Parse the JSON data
        const parsedData = JSON.parse(row.data as string);
        // Ensure _id is included
        if (row._id) {
          parsedData._id = row._id;
        }
        return parsedData as WithId<T>;
      } catch (e) {
        console.error("Error parsing JSON data:", e);
        // If parsing fails, return the row as is
        return row as unknown as WithId<T>;
      }
    }
    
    // Handle the new format with multiple columns
    const doc: Record<string, unknown> = {};
    
    // Process each column
    for (const [key, value] of Object.entries(row)) {
      if (key === '_extra' && typeof value === 'string') {
        // Parse and merge extra fields
        try {
          const extraFields = JSON.parse(value);
          Object.assign(doc, extraFields);
        } catch (e) {
          console.error("Error parsing _extra field:", e);
        }
      } else if (typeof value === 'string' && (
        value.startsWith('{') || 
        value.startsWith('[')
      )) {
        // Try to parse JSON strings
        try {
          doc[key] = JSON.parse(value);
        } catch {
          doc[key] = value;
        }
      } else {
        // Use the value as is
        doc[key] = value;
      }
    }
    
    return doc as WithId<T>;
  }

  /**
   * Applies a projection to a document.
   * 
   * @param doc The document to project
   * @param projection The projection specification
   * @returns The projected document
   */
  private applyProjection<D extends Document>(
    doc: D,
    projection?: Record<string, number | boolean>,
  ): WithId<T> {
    if (!projection || Object.keys(projection).length === 0) {
      return doc as unknown as WithId<T>;
    }
    
    // Determine if this is an inclusion or exclusion projection
    const isInclude = Object.values(projection).some(v => v === 1 || v === true);
    
    // Create a new document with only the _id field
    const result: Record<string, unknown> = { _id: doc._id };
    
    if (isInclude) {
      // Inclusion projection - only include specified fields
      for (const [field, include] of Object.entries(projection)) {
        if (include === 1 || include === true) {
          if (field.includes('.')) {
            // Handle nested fields
            const parts = field.split('.');
            let current = doc as unknown as Record<string, unknown>;
            let target = result;
            
            // Create nested structure in result
            for (let i = 0; i < parts.length - 1; i++) {
              const part = parts[i];
              if (current[part] === undefined) {
                break;
              }
              
              if (target[part] === undefined) {
                target[part] = {};
              }
              
              current = current[part] as Record<string, unknown>;
              target = target[part] as Record<string, unknown>;
            }
            
            const lastPart = parts[parts.length - 1];
            if (current && current[lastPart] !== undefined) {
              target[lastPart] = current[lastPart];
            }
          } else if (doc[field] !== undefined) {
            result[field] = doc[field];
          }
        }
      }
    } else {
      // Exclusion projection - include all fields except specified ones
      // First, copy all fields from the document
      Object.assign(result, doc);
      
      // Then remove excluded fields
      for (const [field, exclude] of Object.entries(projection)) {
        if (exclude === 0 || exclude === false) {
          if (field.includes('.')) {
            // Handle nested fields for exclusion
            const parts = field.split('.');
            let current = result as Record<string, unknown>;
            
            // Navigate to the parent object
            for (let i = 0; i < parts.length - 1; i++) {
              const part = parts[i];
              if (current[part] === undefined) {
                break;
              }
              current = current[part] as Record<string, unknown>;
            }
            
            // Delete the field from its parent
            const lastPart = parts[parts.length - 1];
            if (current && typeof current === 'object') {
              delete current[lastPart];
            }
          } else {
            // Delete top-level field
            delete result[field];
          }
        }
      }
    }
    
    return result as unknown as WithId<T>;
  }
} 