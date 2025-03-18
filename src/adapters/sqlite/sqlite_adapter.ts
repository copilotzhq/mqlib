/**
 * SQLite Adapter implementation for MQLib
 */

import { BaseSqlAdapter, CollectionSchema, FieldDefinition } from "../base_adapter.ts";
import { SqlQuery } from "../../interfaces/adapter.ts";
import { UpdateOperator } from "../../types/update.ts";
import { Filter, FindOptions } from "../../types/filter.ts";

/**
 * SQLite-specific implementation of the SqlAdapter interface.
 */
export class SqliteAdapter extends BaseSqlAdapter {
  /**
   * Gets the name of the SQL dialect.
   */
  readonly dialect = "sqlite";


  protected override isSchemaEnabled = false; // Flag to indicate if schema-based approach is enabled

  /**
   * Escapes a SQL identifier (table name, column name, etc.).
   * 
   * @param identifier The identifier to escape
   * @returns The escaped identifier
   */
  override escapeIdentifier(identifier: string): string {
    // Double quote identifiers for SQLite
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Gets the placeholder syntax for parameterized queries.
   * 
   * @param index The parameter index (0-based)
   * @returns The placeholder string for the specified parameter
   */
  override getParameterPlaceholder(_index: number): string {
    // SQLite uses ? for all parameters
    return '?';
  }

  /**
   * Analyzes a schema object and builds a CollectionSchema representation.
   * 
   * @param tableName The name of the table
   * @param schema The schema object
   * @returns The analyzed schema
   */
  private analyzeSchema(tableName: string, schema: any): CollectionSchema {
    const fields = new Map<string, FieldDefinition>();
    let hasExtraField = false;

    // Helper function to determine SQL type from schema property
    const getSqlType = (property: any): string => {
      const type = property.type;
      if (typeof type === 'string') {
        switch (type.toLowerCase()) {
          case 'string':
            return 'TEXT';
          case 'number':
            return 'REAL';
          case 'integer':
            return 'INTEGER';
          case 'boolean':
            return 'INTEGER'; // SQLite doesn't have a boolean type
          case 'date':
            return 'TEXT'; // Store dates as ISO strings
          default:
            return 'TEXT'; // Default to TEXT for unknown types
        }
      } else if (Array.isArray(type)) {
        // Handle union types - use the most permissive type
        return 'TEXT';
      } else {
        // Default to TEXT for complex types
        return 'TEXT';
      }
    };

    // Helper function to determine if a property is a scalar type
    const isScalar = (property: any): boolean => {
      if (!property.type) return false;
      
      const type = property.type;
      if (typeof type === 'string') {
        const t = type.toLowerCase();
        return t === 'string' || t === 'number' || t === 'integer' || t === 'boolean' || t === 'date';
      }
      return false;
    };

    if (schema.properties) {
      for (const [name, property] of Object.entries<any>(schema.properties)) {
        if (name === '_extra') {
          hasExtraField = true;
          continue;
        }

        const isArray = property.type === 'array';
        
        fields.set(name, {
          type: property.type || 'object',
          isScalar: isScalar(property),
          sqlType: getSqlType(property),
          path: name,
          isArray
        });

        if (property.properties) {
          // Handle nested object - add fields for all properties
          for (const [subName, subProperty] of Object.entries<any>(property.properties)) {
            const path = `${name}.${subName}`;
            fields.set(path, {
              type: subProperty.type || 'unknown',
              isScalar: isScalar(subProperty),
              sqlType: getSqlType(subProperty),
              path,
              isArray: subProperty.type === 'array'
            });
          }
        }
      }
    }

    return {
      tableName,
      fields,
      hasExtraField
    };
  }

  /**
   * Translates a JSON schema to a SQLite CREATE TABLE statement.
   * 
   * @param tableName The name of the table to create
   * @param schema The JSON schema defining the table structure
   * @returns A SQL query object with the CREATE TABLE statement
   */
  override translateSchema(tableName: string, schema: object): SqlQuery {
    // Analyze the schema to determine field types
    const collectionSchema = this.analyzeSchema(tableName, schema);

    // Store the schema for future reference
    this.collectionSchemas.set(tableName, collectionSchema);

    const escapedTableName = this.escapeIdentifier(tableName);
    const columns: string[] = [];

    // Add columns for each field in the schema
    for (const [field, definition] of collectionSchema.fields.entries()) {
      columns.push(`${this.escapeIdentifier(field)} ${definition.sqlType}`);
    }

    // Add _extra column for additional properties if needed
    if (collectionSchema.hasExtraField) {
      columns.push(`${this.escapeIdentifier('_extra')} TEXT`);
    }

    // Build the CREATE TABLE statement
    const sql = `CREATE TABLE IF NOT EXISTS ${escapedTableName} (\n  ${columns.join(',\n  ')}\n)`;

    return { sql, params: [] };
  }

  /**
   * Translates a MongoDB-style index specification to a SQLite CREATE INDEX statement.
   * 
   * @param tableName The name of the table to create an index on
   * @param indexName The name of the index
   * @param fields The fields to include in the index and their sort direction
   * @param options Options for the index
   * @returns A SQL query object with the CREATE INDEX statement
   */
  override translateCreateIndex(
    tableName: string,
    indexName: string,
    fields: Record<string, 1 | -1>,
    options?: { unique?: boolean; sparse?: boolean }
  ): SqlQuery {
    const uniqueClause = options?.unique ? "UNIQUE" : "";
    const indexColumns: string[] = [];

    for (const [field, direction] of Object.entries(fields)) {
      // SQLite doesn't support DESC in index creation, but it doesn't matter
      // since indexes can be used in either direction
      indexColumns.push(this.escapeIdentifier(field));
    }

    // SQLite doesn't support sparse indexes, so we ignore that option

    const sql = `CREATE ${uniqueClause} INDEX IF NOT EXISTS ${this.escapeIdentifier(indexName)} ON ${this.escapeIdentifier(tableName)} (${indexColumns.join(", ")})`;

    return { sql, params: [] };
  }

  /**
   * Overrides the base implementation to handle SQLite-specific REGEXP operator.
   * 
   * @param column The column name
   * @param operators The operators and their values
   * @param params Array to collect parameter values
   * @returns The SQL conditions for the operators
   */
  protected override buildOperatorConditions(column: string, operators: Record<string, unknown>, params: unknown[]): string {
    const conditions: string[] = [];

    for (const [op, value] of Object.entries(operators)) {
      if (op === "$regex") {
        // SQLite doesn't have a built-in REGEXP operator, but it can be added
        // For now, we'll use LIKE with % wildcards as a simple approximation
        const regexStr = String(value);
        let likePattern = regexStr;

        // Convert simple regex patterns to LIKE patterns
        if (regexStr.startsWith("^")) {
          likePattern = likePattern.substring(1);
        } else {
          likePattern = "%" + likePattern;
        }

        if (regexStr.endsWith("$")) {
          likePattern = likePattern.substring(0, likePattern.length - 1);
        } else {
          likePattern = likePattern + "%";
        }

        params.push(likePattern);
        conditions.push(`${column} LIKE ${this.getParameterPlaceholder(params.length - 1)}`);
        continue;
      }

      // Handle array operators
      if (op === "$all" || op === "$elemMatch" || op === "$size") {
        // For SQLite, we need to use JSON1 extension functions
        if (op === "$all" && Array.isArray(value)) {
          // Check if all items in the array are in the JSON array
          const allConditions: string[] = [];

          for (const item of value) {
            params.push(JSON.stringify(item));
            allConditions.push(`json_array_length(json_extract(${column}, '$[?]')) > 0`);
          }

          if (allConditions.length > 0) {
            conditions.push(`(${allConditions.join(" AND ")})`);
          }
          continue;
        }

        if (op === "$size" && typeof value === "number") {
          // Check the length of the JSON array
          params.push(value);
          conditions.push(`json_array_length(${column}) = ${this.getParameterPlaceholder(params.length - 1)}`);
          continue;
        }

        if (op === "$elemMatch" && typeof value === "object" && value !== null) {
          // $elemMatch looks for array elements that match all the specified criteria
          const elemMatchConditions: string[] = [];
          
          for (const [subOp, subValue] of Object.entries(value as Record<string, unknown>)) {
            if (typeof subValue === "object" && subValue !== null && !Array.isArray(subValue)) {
              // Handle operator conditions like { level: { $gte: 4 } }
              for (const [operator, operValue] of Object.entries(subValue as Record<string, unknown>)) {
                switch (operator) {
                  case "$eq":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') = ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  case "$ne":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') != ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  case "$gt":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') > ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  case "$gte":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') >= ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  case "$lt":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') < ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  case "$lte":
                    params.push(operValue);
                    elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') <= ${this.getParameterPlaceholder(params.length - 1)}`);
                    break;
                  default:
                    throw new Error(`Nested operator ${operator} not implemented for $elemMatch in SQLite`);
                }
              }
            } else {
              // Handle direct value comparisons like { name: "Swift" }
              params.push(subValue);
              elemMatchConditions.push(`json_extract(json_each.value, '$.${subOp}') = ${this.getParameterPlaceholder(params.length - 1)}`);
            }
          }
          
          if (elemMatchConditions.length > 0) {
            conditions.push(`EXISTS (
              SELECT 1 FROM json_each(${column}) 
              WHERE ${elemMatchConditions.join(" AND ")}
            )`);
          }
          continue;
        }

        // If we get here, it's an unsupported operator or value type
        throw new Error(`Operator ${op} not fully implemented for SQLite with the provided value type`);
      }

      // Fall back to base implementation for other operators
      try {
        const baseCondition = super.buildOperatorConditions(column, { [op]: value }, params);
        if (baseCondition) {
          conditions.push(baseCondition);
        }
      } catch (error) {
        if (error instanceof Error && error.message.includes("not implemented in base adapter")) {
          throw new Error(`Operator ${op} not implemented for SQLite`);
        }
        throw error;
      }
    }

    return conditions.length > 0 ? conditions.join(" AND ") : "";
  }

  /**
   * Overrides the base implementation to handle SQLite-specific array update operators.
   * 
   * @param update The MongoDB-style update operators
   * @param params Array to collect parameter values
   * @param tableName Optional table name to get schema information
   * @returns The SQL SET clause
   */
  protected override buildSetClause<T>(update: UpdateOperator<T>, params: unknown[], tableName?: string): string {
    // First use the existing implementation for regular fields
    const setClauses: string[] = [];
    
    // Handle $set as before with special handling for array indices
    if (update.$set) {
      const setItems: string[] = [];
      
      for (const [key, value] of Object.entries(update.$set)) {
        // Check if the field has an array index pattern like "skills.1.level"
        if (key.includes('.')) {
          const parts = key.split('.');
          const rootField = parts[0];
          
          // Check if the second part is a number (array index)
          if (parts.length >= 2 && !isNaN(Number(parts[1]))) {
            // We're updating an array element at a specific index
            const arrayIndex = Number(parts[1]);
            
            if (parts.length === 2) {
              // Replace the entire array element
              params.push(arrayIndex);
              params.push(value);
              setClauses.push(`${this.escapeIdentifier(rootField)} = json_set(${this.escapeIdentifier(rootField)}, '$[' || ? || ']', ?)`);
            } else {
              // Replace a property within an array element
              const propertyPath = parts.slice(2).join('.');
              params.push(arrayIndex);
              params.push(value);
              setClauses.push(`${this.escapeIdentifier(rootField)} = json_set(${this.escapeIdentifier(rootField)}, '$[' || ? || '].${propertyPath}', ?)`);
            }
          } else {
            // Regular nested path like "address.city"
            const jsonPath = parts.slice(1).join('.');
            params.push(value);
            setClauses.push(`${this.escapeIdentifier(rootField)} = json_set(${this.escapeIdentifier(rootField)}, '$.${jsonPath}', ?)`);
          }
        } else {
          // Regular field
          params.push(value);
          setClauses.push(`${this.escapeIdentifier(key)} = ?`);
        }
      }
    }
    
    // Implement $inc specially for SQLite
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        params.push(value);
        setClauses.push(`${this.escapeIdentifier(key)} = ${this.escapeIdentifier(key)} + ?`);
      }
    }
    
    // Implement $unset (set to NULL for SQL)
    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        setClauses.push(`${this.escapeIdentifier(key)} = NULL`);
      }
    }
    
    // Handle array operator $push - append to array
    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        // Serialize any objects/arrays to JSON string before binding
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        
        // If array doesn't exist yet, create it. Otherwise append the new value
        params.push(serializedValue);
        params.push(serializedValue);  // Need to add the parameter twice since it's used twice in the CASE statement
        setClauses.push(
          `${this.escapeIdentifier(key)} = CASE 
            WHEN ${this.escapeIdentifier(key)} IS NULL 
            THEN json_array(?) 
            ELSE json_insert(${this.escapeIdentifier(key)}, '$[#]', ?) 
          END`
        );
      }
    }
    
    // Handle array operator $addToSet - add to array if not present
    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        // Serialize any objects/arrays to JSON string before binding
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        
        // More complex case - add only if not exists
        params.push(serializedValue);
        params.push(serializedValue);
        params.push(serializedValue);
        setClauses.push(
          `${this.escapeIdentifier(key)} = CASE 
            WHEN ${this.escapeIdentifier(key)} IS NULL 
            THEN json_array(?) 
            WHEN NOT EXISTS(SELECT 1 FROM json_each(${this.escapeIdentifier(key)}) WHERE json_each.value = ?) 
            THEN json_insert(${this.escapeIdentifier(key)}, '$[#]', ?) 
            ELSE ${this.escapeIdentifier(key)} 
          END`
        );
      }
    }
    
    // Handle array operator $pull - remove elements from an array
    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        // Serialize any objects/arrays to JSON string before binding
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
        
        // Filter the array to remove matching elements
        params.push(serializedValue);
        setClauses.push(
          `${this.escapeIdentifier(key)} = (
              SELECT json_group_array(value) 
            FROM json_each(COALESCE(${this.escapeIdentifier(key)}, json_array())) 
              WHERE value != ?
          )`
        );
      }
    }
    
    // Return just the comma-separated set clauses without the "SET" prefix
    return setClauses.join(', ');
  }

  /**
   * Helper function to recursively parse JSON values
   */
  protected override parseJsonRecursively(value: any): any {
    if (value === null || value === undefined) {
        return value;
      }

    if (Array.isArray(value)) {
      return value.map(item => this.parseJsonRecursively(item));
    }

    if (typeof value === 'object') {
      const result: Record<string, any> = {};
      for (const [key, val] of Object.entries(value)) {
          result[key] = this.parseJsonRecursively(val);
      }
      return result;
    }

    // Special handling for JSON strings that might be nested
    if (typeof value === 'string' && this.mightBeJsonData(value)) {
      try {
        const parsed = JSON.parse(value);
        return this.parseJsonRecursively(parsed);
      } catch (e) {
        // If not valid JSON, return as is
      }
    }

    return value;
  }

  /**
   * Creates a helper method to detect if a value might be JSON based on its format
   */
  protected override mightBeJsonData(value: string): boolean {
    // Check if the value has JSON-like formatting
    return (value.startsWith('[') && value.endsWith(']')) || 
           (value.startsWith('{') && value.endsWith('}'));
  }

  /**
   * Processes the result of a query, mapping column values to document properties.
   */
  override processQueryResult(result: any, tableName: string): { rows: Record<string, unknown>[]; rowCount: number } {
    // If the result is already in the expected format with rows and rowCount, return it as is
    if (result.rows && typeof result.rowCount === 'number') {
      return result;
    }

    // Get the schema for this collection
    const schema = this.getCollectionSchema(tableName);
    const useSchemaBasedApproach = schema && this.isSchemaEnabled;

    // Handle raw SQLite result from the connection factory
    if (result.rawResult) {
      const rows: Record<string, unknown>[] = [];

      if (!useSchemaBasedApproach) {
        // Old format with _id and data columns
        for (const row of result.rawResult) {
          if (Array.isArray(row)) {
          // SQLite returns rows as arrays, so we need to map them to objects
          // For our schema, we expect the first column to be _id and the second to be data
          if (row.length >= 2) {
            const id = row[0];
            const dataStr = row[1];

            try {
              // Parse the JSON data
              const data = JSON.parse(dataStr);
              // Add the _id field
              data._id = id;
              rows.push(data);
            } catch (e) {
              console.error("Error parsing JSON data:", e);
              // If parsing fails, create a basic object with _id
              rows.push({ _id: id, data: dataStr });
            }
            }
          } else if (typeof row === 'object' && row !== null) {
            // Handle object rows for native SQLite
            const rowObj: Record<string, unknown> = {};
            for (const [key, value] of Object.entries(row)) {
              // For key-value object rows, add each property
              rowObj[key] = value;
            }
            rows.push(rowObj);
          }
        }
      } else {
        // New format with multiple columns
        // Get column information from the query result
        const columnNames = result.columnNames || [];

        for (const row of result.rawResult) {
          const rowObj: Record<string, unknown> = {};

          if (Array.isArray(row)) {
          // Map array values to column names
          for (let i = 0; i < Math.min(row.length, columnNames.length); i++) {
            const columnName = columnNames[i];
            let value = row[i];

              // Try to parse JSON values
            if (value !== null && typeof value === 'string') {
                // Skip _id field which may contain UUIDs that would cause JSON parsing errors
                if (columnName !== '_id' && this.mightBeJsonData(value)) {
                  try {
                    const parsed = JSON.parse(value);
                    value = this.parseJsonRecursively(parsed);
                } catch (e) {
                    // Keep the original value if parsing fails
                }
              }
            }

            rowObj[columnName] = value;
          }
          } else if (typeof row === 'object' && row !== null) {
            // Handle object rows (native SQLite implementation)
            for (const [columnName, value] of Object.entries(row)) {
              let processedValue = value;
              
              // Special handling for known array/object fields that might be JSON strings
              if (value !== null && typeof value === 'string') {
                if (columnName !== '_id') {
                  // Try to parse as JSON for likely JSON fields
                  if (columnName === 'skills' || columnName === 'tags' || columnName === 'preferences' || 
                      columnName === 'address' || columnName === 'favorites' || 
                      this.mightBeJsonData(value as string)) {
                    try {
                      const parsed = JSON.parse(value as string);
                      processedValue = this.parseJsonRecursively(parsed);
                    } catch (e) {
                      // Keep the original value if parsing fails
                    }
                  }
                }
              } else if (value !== null && typeof value === 'object') {
                // Ensure nested objects are properly processed
                processedValue = this.parseJsonRecursively(value);
              }
              
              rowObj[columnName] = processedValue;
            }
          }

          // Clean up document by removing redundant dot notation properties
          this.cleanupDotNotationProperties(rowObj);
          rows.push(rowObj);
        }
      }

      return {
        rows,
        rowCount: rows.length
      };
    }

    // Return an empty result if no rows were found
    return { rows: [], rowCount: 0 };
  }

  /**
   * Removes redundant dot notation properties from a result object
   * This prevents duplicating data when we have both the full object
   * and the flattened dot notation properties
   */
  private cleanupDotNotationProperties(obj: Record<string, any>): Record<string, any> {
    // Collect all parent paths (e.g. 'address', 'preferences')
    const parentPaths = new Set<string>();
    
    // Add all possible parent paths to the set
    Object.keys(obj).forEach(key => {
      if (key.includes('.')) {
        const parentPath = key.split('.')[0];
        parentPaths.add(parentPath);
      }
    });

    // Remove all dot notation fields that have a corresponding parent field
    // that is not null (we want to keep dot notation if parent is null)
    parentPaths.forEach(parentPath => {
      if (parentPath in obj && obj[parentPath] !== null) {
        // Remove all dot notation fields with this parent
        Object.keys(obj).forEach(key => {
          if (key.startsWith(`${parentPath}.`)) {
            delete obj[key];
          }
        });
      }
    });

    return obj;
  }

  /**
   * Processes a document before inserting it into the database.
   * 
   * @param doc The document to process
   * @param tableName The name of the table to insert into
   * @returns The processed document with fields mapped to columns
   */
  override processDocumentForInsert<T extends Record<string, unknown>>(doc: T, tableName: string): Record<string, unknown> {
    // Get the schema for this collection
    const schema = this.getCollectionSchema(tableName);
    const useSchemaBasedApproach = schema && this.isSchemaEnabled;

    // If no schema is available or schema-based approach is disabled, fall back to the old behavior
    if (!useSchemaBasedApproach) {
      // For SQLite, we need to convert the document to a format that can be stored in the database
      // We'll store the _id as a separate column and the rest of the document as JSON
      const id = doc._id;
      const data = JSON.stringify(doc);

      // Return the processed document
      return {
        _id: id,
        data: data
      };
    }

    // Process document according to schema
    const result: Record<string, unknown> = {};
    const extraFields: Record<string, unknown> = {};

    // Process each field in the document
    for (const [key, value] of Object.entries(doc)) {
      const fieldDef = schema.fields.get(key);

      if (fieldDef) {
        // Field is in the schema
        if (fieldDef.isScalar) {
          // Store scalar values directly
          result[key] = value;
        } else {
          // Store complex values as JSON
          result[key] = JSON.stringify(value);
        }
      } else if (key !== '_id') {
        // Field is not in the schema, add to extra fields
        extraFields[key] = value;
      }
    }

    // Always ensure _id is included
    if (!result._id && doc._id) {
      result._id = doc._id;
    }

    // Add extra fields as JSON if needed
    if (schema.hasExtraField && Object.keys(extraFields).length > 0) {
      result._extra = JSON.stringify(extraFields);
    }

    return result;
  }

  /**
   * Translates a MongoDB-style document to a SQL INSERT statement.
   * 
   * @param tableName The name of the table to insert into
   * @param document The document to insert
   * @returns A SQL query object with the INSERT statement and parameters
   */
  override translateInsert<T>(tableName: string, document: T): SqlQuery {
    const escapedTableName = this.escapeIdentifier(tableName);
    const params: unknown[] = [];

    // Process the document for insertion
    const processedDoc = this.processDocumentForInsert(document as unknown as Record<string, unknown>, tableName);

    // Get the schema for this collection
    const schema = this.getCollectionSchema(tableName);

    if (!schema) {
      // Fall back to the old behavior for collections without a schema
      const sql = `INSERT INTO ${escapedTableName} ("_id", "data") VALUES (?, ?)`;

      // Add the parameters
      params.push(processedDoc._id);
      params.push(processedDoc.data);

      return { sql, params };
    }

    // Build the column list and value placeholders
    const columns: string[] = [];
    const placeholders: string[] = [];

    for (const [key, value] of Object.entries(processedDoc)) {
      if (value !== undefined) {
        columns.push(this.escapeIdentifier(key));
        placeholders.push(this.getParameterPlaceholder(params.length));
        params.push(value);
      }
    }

    // Build the INSERT statement
    const sql = `INSERT INTO ${escapedTableName} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;

    return { sql, params };
  }

  /**
   * Handle array field queries generically, replacing the tags-specific logic
   * This replaces the tags-specific method with a generalized approach for any array field
   * 
   * @param whereClause The SQL WHERE clause to process
   * @param params The query parameters
   * @returns A processed SQL query
   */
  private handleArrayFieldQuery(whereClause: string, params: unknown[], tableName: string = ""): SqlQuery {
    if (!whereClause || whereClause.trim() === '') {
      return { sql: whereClause, params };
    }

    let processedSql = whereClause;
    let processedParams = [...params];
    
    // Handle all array fields generically using regex patterns
    let updatedSql = processedSql;
    const newParams: unknown[] = [];
    
    // Find all patterns like "field" IN (?) where params are arrays
    const inClauseRegex = /"([^"]+)" IN \(\?\)/g;
    let match;
    let paramIndex = 0;
    let sqlWithReplacements = processedSql;
    
    while ((match = inClauseRegex.exec(processedSql)) !== null) {
      const fieldName = match[1];
      const matchStart = match.index;
      
      // Check if the current field is an array field and if the parameter is an array
      if (this.isArrayField(tableName, fieldName) && 
          paramIndex < processedParams.length && 
          Array.isArray(processedParams[paramIndex])) {
        
        const arrayValues = processedParams[paramIndex] as unknown[];
        const placeholders = arrayValues.map(() => '?').join(', ');
        
        // Build the replacement
        const replacement = `EXISTS (SELECT 1 FROM json_each("${fieldName}") WHERE value IN (${placeholders}))`;
        
        // Update SQL with replacement
        const beforeMatch = sqlWithReplacements.substring(0, matchStart);
        const afterMatch = sqlWithReplacements.substring(matchStart + match[0].length);
        sqlWithReplacements = beforeMatch + replacement + afterMatch;
        
        // Add the individual array values to new params
        for (const value of arrayValues) {
          newParams.push(value);
        }
      } else {
        // Keep the parameter as is
        newParams.push(processedParams[paramIndex]);
      }
      
      paramIndex++;
    }
    
    // Find all patterns like "field" = ? where field might be an array field
    const equalityRegex = /"([^"]+)" = \?/g;
    paramIndex = 0;
    
    // Reset regex
    equalityRegex.lastIndex = 0;
    
    while ((match = equalityRegex.exec(processedSql)) !== null) {
      const fieldName = match[1]; // Field name
      
      // Check if this is an array field
      if (this.isArrayField(tableName, fieldName)) {
        const matchStart = match.index;
        
        // Build the replacement - JSON array containment check
        const replacement = `EXISTS (SELECT 1 FROM json_each("${fieldName}") WHERE value = ?)`;
        
        // Update SQL with replacement
        const beforeMatch = sqlWithReplacements.substring(0, matchStart);
        const afterMatch = sqlWithReplacements.substring(matchStart + match[0].length);
        sqlWithReplacements = beforeMatch + replacement + afterMatch;
      }
      
      // We keep the parameter unchanged in this case
    }
    
    // If we made changes to the SQL, use the updated version and params
    if (sqlWithReplacements !== processedSql) {
      return { sql: sqlWithReplacements, params: newParams.length > 0 ? newParams : processedParams };
    }
    
    return { sql: processedSql, params: processedParams };
  }

  /**
   * Translates a MongoDB-style filter to a SQL WHERE clause.
   * 
   * @param filter The MongoDB-style filter
   * @param tableName The name of the table 
   * @returns A SQL query object with the WHERE clause and parameters
   */
  override translateFilter<T>(filter: Filter<T>, tableName: string = ""): SqlQuery {
    
    // If filter is empty, return an empty WHERE clause
    if (!filter || Object.keys(filter).length === 0) {
      return { sql: "", params: [] };
    }
    
    // Get schema if available
    const schema = this.getCollectionSchema(tableName);
    
    // Handle special cases in the filter
    const processedFilter = this.preprocessFilter(filter, tableName);
    
    // Convert dot notation paths like 'address.city' to nested objects
    const finalFilter = this.handleNestedPathInFilter(processedFilter);
    
    // Build the WHERE clause
    let whereClause = "";
    const params: unknown[] = [];
    
    // Use schema-based approach if schema is available and enabled
    if (schema && this.isSchemaEnabled) {
      whereClause = this.buildSchemaBasedWhereClause(finalFilter as Record<string, unknown>, schema, params);
    } else {
      // Fall back to generic approach
      const [conditions, conditionParams] = this.buildWhereConditionsForFilter(tableName, finalFilter as Record<string, unknown>);
      whereClause = conditions.join(" AND ");
      params.push(...conditionParams);
    }
    
    
    // Only add the WHERE keyword if there's a condition
    if (whereClause && whereClause.trim() !== "") {
      whereClause = `WHERE ${whereClause}`;
    }
    
    // Process the WHERE clause for array field queries
    const processedQuery = this.handleArrayFieldQuery(whereClause, params, tableName);
    
    const result = { 
      sql: processedQuery.sql, 
      params: processedQuery.params 
    };
    
    return result;
  }

  /**
   * Preprocess a filter to handle special cases like string values for array fields
   */
  private preprocessFilter<T>(filter: Filter<T>, tableName: string): Filter<T> {
    if (!filter || typeof filter !== 'object') {
      return filter;
    }

    // Create a shallow copy of the filter
    const result = { ...filter } as Record<string, unknown>;

    // Detect and transform string values for array fields
    for (const [key, value] of Object.entries(filter)) {
      if (typeof value === 'string' && this.isArrayField(tableName, key)) {
        // For array fields with string values, transform to use $in operator
        result[key] = { $in: [value] };
      } else if (value && typeof value === 'object' && !Array.isArray(value)) {
        // Recursively process nested objects (but not arrays)
        result[key] = this.preprocessFilter(value as Filter<unknown>, tableName);
      }
    }

    return result as Filter<T>;
  }

  /**
   * Builds a WHERE clause for a MongoDB-style filter using the schema information.
   * 
   * @param filter The MongoDB-style filter
   * @param schema The collection schema
   * @param params Array to collect parameter values
   * @returns The WHERE clause
   */
  private buildSchemaBasedWhereClause(filter: Record<string, unknown>, schema: CollectionSchema, params: unknown[]): string {
    const conditions: string[] = [];
    const tableName = schema.tableName; // Define the tableName variable

    for (const [key, condition] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // This is a logical operator, handle it specially
        if (key === '$and' && Array.isArray(condition)) {
          const andConditions = condition
            .map((subFilter) => this.buildSchemaBasedWhereClause(subFilter as Record<string, unknown>, schema, params))
            .filter((cond) => cond !== '');

          if (andConditions.length > 0) {
            conditions.push(`(${andConditions.join(' AND ')})`);
          }
        } else if (key === '$or' && Array.isArray(condition)) {
          const orConditions = condition
            .map((subFilter) => this.buildSchemaBasedWhereClause(subFilter as Record<string, unknown>, schema, params))
            .filter((cond) => cond !== '');

          if (orConditions.length > 0) {
            conditions.push(`(${orConditions.join(' OR ')})`);
          }
        } else if (key === '$nor' && Array.isArray(condition)) {
          const norConditions = condition
            .map((subFilter) => this.buildSchemaBasedWhereClause(subFilter as Record<string, unknown>, schema, params))
            .filter((cond) => cond !== '');

          if (norConditions.length > 0) {
            conditions.push(`NOT (${norConditions.join(' OR ')})`);
          }
        } else if (key === '$not' && typeof condition === 'object' && condition !== null) {
          const notCondition = this.buildSchemaBasedWhereClause(condition as Record<string, unknown>, schema, params);
          if (notCondition !== '') {
            conditions.push(`NOT (${notCondition})`);
          }
        }
        continue;
      }

      // Check if this is a nested field
      if (key.includes('.')) {
        const parts = key.split('.');
        const columnName = parts[0];
        const jsonPath = parts.slice(1).join('.');

        // Check if the root field exists in the schema
        const fieldDef = schema.fields.get(columnName);

        if (fieldDef && !fieldDef.isScalar) {
          // This is a JSON column, query it with json_extract
          if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
            // This is an operator condition
            for (const [op, opValue] of Object.entries(condition as Record<string, unknown>)) {
              switch (op) {
                case '$eq':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') = ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$ne':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') != ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$gt':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') > ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$gte':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') >= ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$lt':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') < ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$lte':
                  params.push(opValue);
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') <= ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$exists':{
                  const existsCondition = opValue ? 'IS NOT NULL' : 'IS NULL';
                  conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') ${existsCondition}`);
                  break;
                }
              }
            }
          } else {
            // For JSON arrays of objects (like skills), we need special handling
            if (parts.length > 1 && this.isArrayField(tableName, columnName)) {
              // We're likely dealing with an array of objects
              const valueToMatch = condition;
              params.push(valueToMatch);
              conditions.push(`EXISTS (
                SELECT 1 FROM json_each(${this.escapeIdentifier(columnName)}) 
                WHERE json_extract(json_each.value, '$.${parts.slice(1).join('.')}') = ${this.getParameterPlaceholder(params.length - 1)}
              )`);
            } else {
              // Standard JSON field comparison for nested objects like address.city or preferences.theme
              const valueToMatch = condition;
              params.push(valueToMatch);
              conditions.push(`json_extract(${this.escapeIdentifier(columnName)}, '$.${jsonPath}') = ${this.getParameterPlaceholder(params.length - 1)}`);
            }
          }
        } else if (schema.hasExtraField) {
          // Try in the _extra field
          const valueToMatch = condition;
          params.push(valueToMatch);
          conditions.push(`json_extract(${this.escapeIdentifier('_extra')}, '$.${key}') = ${this.getParameterPlaceholder(params.length - 1)}`);
        }
      } else {
        // Check if the field exists in the schema
        const fieldDef = schema.fields.get(key);
        if (fieldDef) {
          // This is a direct column
          if (fieldDef.isScalar) {
            // This is a scalar field
            if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
              // This is an operator condition
              const conditionObj = this.buildOperatorConditions(key, condition as Record<string, unknown>, params);
              if (conditionObj) {
                conditions.push(conditionObj);
              }
            } else {
              // This is a direct value comparison
              params.push(condition);
              conditions.push(`${this.escapeIdentifier(key)} = ${this.getParameterPlaceholder(params.length - 1)}`);
            }
          } else if (Array.isArray(condition)) {
            // We're looking for a value in an array field
            if (condition.length === 1) {
              // If we have a single value, use the EXISTS with json_each approach
              conditions.push(this.buildArrayContainsValueClause(key, condition[0], params));
            } else {
              // Multiple values - check if any match
              const subConditions: string[] = [];
              for (const item of condition) {
                subConditions.push(this.buildArrayContainsValueClause(key, item, params));
              }
              conditions.push(`(${subConditions.join(' OR ')})`);
            }
          } else if (typeof condition === 'object' && condition !== null) {
            // This is a more complex condition for a JSON field
            for (const [op, value] of Object.entries(condition as Record<string, unknown>)) {
              switch (op) {
                case '$eq':
                  params.push(value);
                  conditions.push(`json_extract(${this.escapeIdentifier(key)}, '$') = ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$ne':
                  params.push(value);
                  conditions.push(`json_extract(${this.escapeIdentifier(key)}, '$') != ${this.getParameterPlaceholder(params.length - 1)}`);
                  break;
                case '$in':
                  if (Array.isArray(value)) {
                    if (value.length === 0) {
                      conditions.push('0 = 1'); // Always false
                    } else {
                      const orClauses: string[] = [];
                      for (const item of value) {
                        // For JSON arrays like "tags", we need a special handling to check if the array contains the value
                        if (this.isArrayField(tableName, key)) {
                          params.push(item);
                          orClauses.push(`EXISTS (
                            SELECT 1 FROM json_each(${this.escapeIdentifier(key)})
                            WHERE json_each.value = ${this.getParameterPlaceholder(params.length - 1)}
                          )`);
                        } else {
                          // For regular fields, a simple equality check
                          params.push(item);
                          orClauses.push(`${this.escapeIdentifier(key)} = ${this.getParameterPlaceholder(params.length - 1)}`);
                        }
                      }
                      if (orClauses.length > 0) {
                        conditions.push(`(${orClauses.join(' OR ')})`);
                      }
                    }
                  }
                  break;
                case '$nin':
                  if (Array.isArray(value)) {
                    if (value.length === 0) {
                      conditions.push('1 = 1'); // Always true
                    } else {
                      const notClauses: string[] = [];
                      for (const item of value) {
                        params.push(JSON.stringify(item));
                        notClauses.push(`NOT (${this.escapeIdentifier(key)} = ${this.getParameterPlaceholder(params.length - 1)})`);
                      }
                      conditions.push(`(${notClauses.join(' AND ')})`);
                    }
                  }
                  break;
                case '$exists': {
                  const existsCondition = value ? 'IS NOT NULL' : 'IS NULL';
                  conditions.push(`json_extract(${this.escapeIdentifier(key)}, '$') ${existsCondition}`);
                  break;
                }
                case '$elemMatch': {
                  if (typeof value === 'object' && value !== null) {
                    // Handle $elemMatch for array fields
                    const elemMatchConditions: string[] = [];
                    
                    for (const [subKey, subValue] of Object.entries(value as Record<string, unknown>)) {
                      if (typeof subValue === 'object' && subValue !== null && !Array.isArray(subValue)) {
                        // Handle operator conditions like { level: { $gte: 4 } }
                        for (const [subOp, subOpValue] of Object.entries(subValue as Record<string, unknown>)) {
                          switch (subOp) {
                            case '$eq':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') = ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            case '$ne':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') != ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            case '$gt':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') > ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            case '$gte':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') >= ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            case '$lt':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') < ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            case '$lte':
                              params.push(subOpValue);
                              elemMatchConditions.push(`json_extract(value, '$.${subKey}') <= ${this.getParameterPlaceholder(params.length - 1)}`);
                              break;
                            default:
                              throw new Error(`Nested operator ${subOp} not implemented for $elemMatch in SQLite`);
                          }
                        }
                      } else {
                        // Handle direct value comparisons like { name: "Swift" }
                        params.push(subValue);
                        elemMatchConditions.push(`json_extract(value, '$.${subKey}') = ${this.getParameterPlaceholder(params.length - 1)}`);
                      }
                    }
                    
                    if (elemMatchConditions.length > 0) {
                      conditions.push(`EXISTS (
                        SELECT 1 FROM json_each(${this.escapeIdentifier(key)}) 
                        WHERE ${elemMatchConditions.join(' AND ')}
                      )`);
                    }
                  }
                  break;
                }
              }
            }
          }
        }
      }
    }

    return conditions.length > 0 ? conditions.join(' AND ') : '';
  }

  /**
   * Builds a SQL condition to check if an array field contains a specific value.
   * 
   * @param fieldName The name of the array field
   * @param value The value to check for
   * @param params The parameters array to append to
   * @returns A SQL condition string
   */
  private buildArrayContainsValueClause(fieldName: string, value: unknown, params: unknown[]): string {
    params.push(value);
    return `EXISTS (SELECT 1 FROM json_each(${this.escapeIdentifier(fieldName)}) WHERE value = ${this.getParameterPlaceholder(params.length - 1)})`;
  }

  /**
   * Builds WHERE conditions for a filter object.
   * 
   * @param tableName The name of the table
   * @param filter The filter object
   * @returns A tuple of [conditions, params]
   */
  private buildWhereConditionsForFilter(tableName: string, filter: Record<string, unknown>): [string[], unknown[]] {
    const conditions: string[] = [];
    const params: unknown[] = [];

    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // This is an operator at the top level
        const opConditions = this.buildOperatorConditions('', value as Record<string, unknown>, params);
        if (opConditions) {
          conditions.push(opConditions);
        }
      } else if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        // Check if the value object contains any operator keys
        const hasOperatorKeys = Object.keys(value as Record<string, unknown>).some(k => k.startsWith('$'));
        
        if (hasOperatorKeys) {
          // Process operators for this field
          for (const [opKey, opValue] of Object.entries(value as Record<string, unknown>)) {
            if (opKey.startsWith('$')) {
              // Handle the operator condition
              const columnName = this.escapeIdentifier(key);
              const opConditions = this.buildOperatorConditions(columnName, { [opKey]: opValue } as Record<string, unknown>, params);
              if (opConditions) {
                conditions.push(opConditions);
              }
            }
          }
        } else {
          // This is a nested object with direct field comparisons
          // For example: { address: { city: 'New York' } }
          const nestedConditions: string[] = [];
          
          for (const [nestedKey, nestedValue] of Object.entries(value as Record<string, unknown>)) {
            // Add the condition for this nested field
            params.push(nestedValue);
            nestedConditions.push(`json_extract(${this.escapeIdentifier(key)}, '$.${nestedKey}') = ${this.getParameterPlaceholder(params.length - 1)}`);
          }
          
          if (nestedConditions.length > 0) {
            conditions.push(`(${nestedConditions.join(' AND ')})`);
          }
        }
      } else if (Array.isArray(value)) {
        // Array of values for a field
        if (this.isArrayField(tableName, key)) {
          // This field is an array in the database, use array containment logic
          const placeholders = value.map(() => '?').join(', ');
          conditions.push(`EXISTS (SELECT 1 FROM json_each(${this.escapeIdentifier(key)}) WHERE json_each.value IN (${placeholders}))`);
          params.push(...value);
        } else {
          // Regular field with an array value means IN operator
          const placeholders = value.map(() => '?').join(', ');
          conditions.push(`${this.escapeIdentifier(key)} IN (${placeholders})`);
          params.push(...value);
        }
      } else {
        // Check if this is a nested path (contains dots)
        if (key.includes('.')) {
          // Handle nested path like 'address.city'
          const parts = key.split('.');
          const rootField = parts[0];
          const jsonPath = parts.slice(1).join('.');
          
          // Add the condition based on whether it's an array field or not
          this.addPathCondition(conditions, params, rootField, jsonPath, value, tableName);
        } else {
          // Simple field equality
          params.push(value);
          conditions.push(`${this.escapeIdentifier(key)} = ${this.getParameterPlaceholder(params.length - 1)}`);
        }
      }
    }

    return [conditions, params];
  }

  /**
   * Adds a condition for a nested path in a JSON field.
   * 
   * @param conditions Array to collect conditions
   * @param params Array to collect parameter values
   * @param rootField The root field name
   * @param jsonPath The nested JSON path
   * @param value The value to compare with
   * @param tableName The name of the table
   */
  private addPathCondition(conditions: string[], params: unknown[], rootField: string, jsonPath: string, value: unknown, tableName: string = ""): void {
    // Handle array fields with nested properties differently
    if (this.isArrayField(tableName, rootField)) {
      // For arrays of objects, we need to use EXISTS with json_each
      params.push(value);
      conditions.push(`EXISTS (
        SELECT 1 FROM json_each(${this.escapeIdentifier(rootField)}) 
        WHERE json_extract(json_each.value, '$.${jsonPath}') = ${this.getParameterPlaceholder(params.length - 1)}
      )`);
    } else {
      // For regular objects, use json_extract
      params.push(value);
      conditions.push(`json_extract(${this.escapeIdentifier(rootField)}, '$.${jsonPath}') = ${this.getParameterPlaceholder(params.length - 1)}`);
    }
  }

  /**
   * Translates a MongoDB-style find operation to a SQLite SELECT statement.
   * 
   * @param tableName The name of the table to select from
   * @param filter The MongoDB-style filter to select documents
   * @param options Options for the find operation
   * @returns A SQL query object with the SELECT statement and parameters
   */
  override translateFind<T>(tableName: string, filter: Filter<T>, options: any = {}): SqlQuery {
    // Start with SELECT * FROM table
    const selectClause = '*';
    
    // Handle sorting with support for nested fields
    let orderByClause = '';
    if (options.sort) {
      const orderParts: string[] = [];
      
      for (const [field, direction] of Object.entries(options.sort)) {
        // Handle nested fields for sorting
        if (field.includes('.')) {
          const parts = field.split('.');
          const rootField = parts[0];
          const jsonPath = parts.slice(1).join('.');
          
          orderParts.push(`json_extract(${this.escapeIdentifier(rootField)}, '$.${jsonPath}') ${direction === 1 ? 'ASC' : 'DESC'}`);
        } else {
          orderParts.push(`${this.escapeIdentifier(field)} ${direction === 1 ? 'ASC' : 'DESC'}`);
        }
      }
      
      if (orderParts.length > 0) {
        orderByClause = `ORDER BY ${orderParts.join(', ')}`;
      }
    }
    
    // Build the LIMIT and OFFSET clauses
    let limitClause = '';
    let offsetClause = '';
    
    if (options.limit !== undefined) {
      limitClause = `LIMIT ${options.limit}`;
    } else if (options.skip !== undefined && options.skip > 0) {
      // SQLite requires a LIMIT clause if OFFSET is used
      limitClause = 'LIMIT 9223372036854775807'; // SQLite max integer
    }
    
    if (options.skip !== undefined && options.skip > 0) {
      offsetClause = `OFFSET ${options.skip}`;
    }
    
    // Handle special cases in the filter
    const processedFilter = this.preprocessFilter(filter, tableName);
    
    // Convert dot notation paths like 'address.city' to nested objects
    const finalFilter = this.handleNestedPathInFilter(processedFilter);
    
    // Build the WHERE clause from the processed filter
    const whereResult = this.translateFilter(finalFilter, tableName);
    let whereClause = whereResult.sql;
    const params = [...whereResult.params];
    
    // Process the WHERE clause for array field queries
    if (whereClause && whereClause.trim() !== '') {
      const processedQuery = this.handleArrayFieldQuery(whereClause, params, tableName);
      whereClause = processedQuery.sql;
      params.length = 0; // Clear params array
      params.push(...processedQuery.params); // Use the processed params
    }
    
    // Build the final SQL query
    let sql = `SELECT ${selectClause} FROM ${this.escapeIdentifier(tableName)}`;
    
    // Only add the WHERE clause if it's not empty or just "WHERE"
    if (whereClause && whereClause.trim() !== 'WHERE' && whereClause.trim() !== 'WHERE ()') {
      sql += ` ${whereClause}`;
    }
    
    if (orderByClause) {
      sql += ` ${orderByClause}`;
    }
    
    if (limitClause) {
      sql += ` ${limitClause}`;
    }
    
    if (offsetClause) {
      sql += ` ${offsetClause}`;
    }
    
    return { sql, params };
  }

  /**
   * Processes filters that contain dot notation paths.
   * 
   * @param filter The MongoDB-style filter
   * @returns A new filter with nested paths converted to the appropriate format
   */
  protected override handleNestedPathInFilter<T>(filter: Filter<T>): Filter<T> {
    if (!filter || typeof filter !== 'object') {
      return filter;
    }

    const newFilter: Record<string, unknown> = {};
    
    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // Handle logical operators
        if (key === '$and' || key === '$or' || key === '$nor') {
          // Process each item in the array
          newFilter[key] = (value as Filter<T>[]).map(item => this.handleNestedPathInFilter(item));
        } else if (key === '$not') {
          // Process the nested filter
          newFilter[key] = this.handleNestedPathInFilter(value as Filter<T>);
        } else {
          // Other operators, keep as is
          newFilter[key] = value;
        }
      } else if (key.includes('.')) {
        // This is a nested path
        const parts = key.split('.');
        const rootField = parts[0];
        
        // If it could be an array index (second part is a number)
        if (parts.length >= 2 && !isNaN(Number(parts[1]))) {
          // Keep it as is, as we'll handle array indices differently
          newFilter[key] = value;
        } 
        // For dot notation, we'll keep the original format for better query handling
        else {
          // Keep the original dot notation
          newFilter[key] = value;
        }
      } else {
        // Regular key, keep as is
        newFilter[key] = value;
      }
    }
    
    return newFilter as Filter<T>;
  }

  /**
   * Determine if a field is an array type based on schema or name convention
   */
  protected override isArrayField(tableName: string, fieldName: string): boolean {
    // Check if we have schema information for this field
    const schema = this.getCollectionSchema(tableName);

    if (schema && schema.fields.has(fieldName)) {
      return schema.fields.get(fieldName)?.isArray || false;
    }

    // If no schema available, check some common array field names
    return ['tags', 'skills', 'favorites', 'categories'].includes(fieldName);
  }

  // Determine if a field contains complex objects (not primitive values) based on schema or data analysis
  protected override isComplexObjectField(tableName: string, fieldName: string): boolean {
    // Check if we have schema information for this field
    const schema = this.getCollectionSchema(tableName);

    if (schema && schema.fields.has(fieldName)) {
      const field = schema.fields.get(fieldName);
      return field ? !field.isScalar : false;
    }

    // If no schema available, check some common object field names
    return ['address', 'preferences', 'options', 'metadata'].includes(fieldName);
  }

  /**
   * Builds a SET clause for an array element at a specific index
   * SQLite-specific implementation
   */
  protected override buildArrayElementSetClause(
    field: string,
    index: number, 
    propertyPath: string | null,
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void {
    if (propertyPath === null) {
      // Replace the entire array element
      params.push(index);
      params.push(value);
      setClauses.push(`${this.escapeIdentifier(field)} = json_set(${this.escapeIdentifier(field)}, '$[' || ? || ']', ?)`);
      } else {
      // Replace a property within an array element
      params.push(index);
      params.push(value);
      setClauses.push(`${this.escapeIdentifier(field)} = json_set(${this.escapeIdentifier(field)}, '$[' || ? || '].${propertyPath}', ?)`);
    }
  }

  /**
   * Builds a SET clause for a nested object property
   * SQLite-specific implementation
   */
  protected override buildObjectPropertySetClause(
    field: string,
    propertyPath: string,
    value: unknown,
    setClauses: string[],
    params: unknown[]
  ): void {
    // Regular nested path like "address.city"
    params.push(value);
    setClauses.push(`${this.escapeIdentifier(field)} = json_set(${this.escapeIdentifier(field)}, '$.${propertyPath}', ?)`);
  }

  /**
   * Builds a SET clause for a $push operation
   * SQLite-specific implementation
   */
  protected override buildPushOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void {
    // Serialize any objects/arrays to JSON string before binding
    const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
    
    // If array doesn't exist yet, create it. Otherwise append the new value
    params.push(serializedValue);
    params.push(serializedValue);  // Need to add the parameter twice since it's used twice in the CASE statement
    setClauses.push(
      `${this.escapeIdentifier(field)} = CASE 
        WHEN ${this.escapeIdentifier(field)} IS NULL 
        THEN json_array(?) 
        ELSE json_insert(${this.escapeIdentifier(field)}, '$[#]', ?) 
      END`
    );
  }

  /**
   * Builds a SET clause for an $addToSet operation
   * SQLite-specific implementation
   */
  protected override buildAddToSetOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void {
    // Serialize any objects/arrays to JSON string before binding
    const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
    
    // More complex case - add only if not exists
    params.push(serializedValue);
    params.push(serializedValue);
    params.push(serializedValue);
    setClauses.push(
      `${this.escapeIdentifier(field)} = CASE 
        WHEN ${this.escapeIdentifier(field)} IS NULL 
        THEN json_array(?) 
        WHEN NOT EXISTS(SELECT 1 FROM json_each(${this.escapeIdentifier(field)}) WHERE json_each.value = ?) 
        THEN json_insert(${this.escapeIdentifier(field)}, '$[#]', ?) 
        ELSE ${this.escapeIdentifier(field)} 
      END`
    );
  }

  /**
   * Builds a SET clause for a $pull operation
   * SQLite-specific implementation
   */
  protected override buildPullOperation(
    field: string, 
    value: unknown, 
    setClauses: string[], 
    params: unknown[]
  ): void {
    // Serialize any objects/arrays to JSON string before binding
    const serializedValue = typeof value === 'object' ? JSON.stringify(value) : value;
    
    // Filter the array to remove matching elements
    params.push(serializedValue);
    setClauses.push(
      `${this.escapeIdentifier(field)} = (
          SELECT json_group_array(value) 
        FROM json_each(COALESCE(${this.escapeIdentifier(field)}, json_array())) 
          WHERE value != ?
      )`
    );
  }
}