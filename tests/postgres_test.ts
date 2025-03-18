/**
 * Test suite for MQLib using PostgreSQL adapter
 * 
 * This file contains tests for the core functionality of MQLib
 * using the PostgreSQL adapter with a mock connection.
 */

import { assertEquals, assertNotEquals, assertExists } from "https://deno.land/std@0.204.0/testing/asserts.ts";
import { Database } from "../src/core/database.ts";
import { PostgresAdapter } from "../src/adapters/postgres/postgres_adapter.ts";
import { Document, DocumentId } from "../src/types/document.ts";

// Create a mock PostgreSQL connection that logs queries and returns mock data
const createMockConnection = () => {
  // Store created tables and their data
  const tables: Record<string, any[]> = {};
  // Store created indexes
  const indexes: Record<string, any[]> = {};
  // Store the last generated ID
  let lastId = 0;

  return {
    query: async (sql: string, params: unknown[] = []) => {
      console.log("Executing SQL:", sql);
      console.log("Parameters:", params);

      // Handle CREATE TABLE
      if (sql.startsWith("CREATE TABLE")) {
        const tableName = sql.match(/CREATE TABLE IF NOT EXISTS "([^"]+)"/)?.[1];
        if (tableName) {
          tables[tableName] = [];
          return { rowCount: 0 };
        }
      }

      // Handle CREATE INDEX
      if (sql.startsWith("CREATE") && sql.includes("INDEX")) {
        const indexName = sql.match(/INDEX IF NOT EXISTS "([^"]+)"/)?.[1];
        const tableName = sql.match(/ON "([^"]+)"/)?.[1];
        if (indexName && tableName) {
          if (!indexes[tableName]) {
            indexes[tableName] = [];
          }
          indexes[tableName].push(indexName);
          return { rowCount: 0 };
        }
      }

      // Handle INSERT
      if (sql.startsWith("INSERT INTO")) {
        const tableName = sql.match(/INSERT INTO "([^"]+)"/)?.[1];
        if (tableName && tables[tableName]) {
          // Generate an ID if not provided
          const doc: Record<string, any> = {};
          const columns = sql.match(/\(([^)]+)\)/)?.[1].split(", ").map(c => c.replace(/"/g, ""));
          
          if (columns) {
            for (let i = 0; i < columns.length; i++) {
              const column = columns[i];
              let value = params[i];
              
              // Generate ID if not provided
              if (column === "_id" && !value) {
                lastId++;
                value = `id_${lastId}`;
              }
              
              doc[column] = value;
            }
            
            // Ensure _id exists
            if (!doc._id) {
              lastId++;
              doc._id = `id_${lastId}`;
            }
            
            tables[tableName].push(doc);
            return { 
              rowCount: 1,
              rows: [doc]
            };
          }
        }
      }

      // Handle SELECT for finding documents
      if (sql.startsWith("SELECT") && !sql.includes("COUNT")) {
        const tableName = sql.match(/FROM "([^"]+)"/)?.[1];
        if (tableName && tables[tableName]) {
          // Very simple mock - in a real implementation we would parse the WHERE clause
          // For testing, we'll just return all documents from the table
          return { 
            rows: tables[tableName],
            rowCount: tables[tableName].length
          };
        }
      }

      // Handle SELECT COUNT
      if (sql.includes("COUNT")) {
        const tableName = sql.match(/FROM "([^"]+)"/)?.[1];
        if (tableName && tables[tableName]) {
          return { 
            rows: [{ count: tables[tableName].length }],
            rowCount: 1
          };
        }
      }

      // Handle UPDATE
      if (sql.startsWith("UPDATE")) {
        const tableName = sql.match(/UPDATE "([^"]+)"/)?.[1];
        if (tableName && tables[tableName]) {
          // Simple mock - just return that 1 row was updated
          return { rowCount: 1 };
        }
      }

      // Handle DELETE
      if (sql.startsWith("DELETE")) {
        const tableName = sql.match(/FROM "([^"]+)"/)?.[1];
        if (tableName && tables[tableName]) {
          // Simple mock - just return that 1 row was deleted
          return { rowCount: 1 };
        }
      }

      // Handle information_schema queries for listCollections
      if (sql.includes("information_schema.tables")) {
        return {
          rows: Object.keys(tables).map(name => ({ table_name: name })),
          rowCount: Object.keys(tables).length
        };
      }

      // Handle DROP TABLE
      if (sql.startsWith("DROP TABLE")) {
        const tableName = sql.match(/DROP TABLE IF EXISTS "([^"]+)"/)?.[1];
        if (tableName) {
          delete tables[tableName];
          return { rowCount: 0 };
        }
      }

      // Default response
      return { rows: [], rowCount: 0 };
    }
  };
};

// Define a test document interface
interface Product extends Document {
  name: string;
  price: number;
  category: string;
  in_stock: boolean;
}

// Define a test schema
const productSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    price: { type: "number", minimum: 0 },
    category: { type: "string" },
    in_stock: { type: "boolean" }
  },
  required: ["_id", "name", "price", "category", "in_stock"]
};

Deno.test("MQLib PostgreSQL - Basic CRUD Operations", async () => {
  const connection = createMockConnection();
  const adapter = new PostgresAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const products = await db.createCollection<Product>("products", productSchema);
  
  // Verify the collection was created
  assertExists(products);
  assertEquals(products.name, "products");
  
  // Insert a document
  const insertResult = await products.insertOne({
    name: "Laptop",
    price: 999.99,
    category: "electronics",
    in_stock: true
  });
  
  // Verify the insert result
  assertExists(insertResult);
  assertExists(insertResult.insertedId);
  assertEquals(insertResult.acknowledged, true);
  
  // Find the document
  const laptop = await products.findOne({ name: "Laptop" });
  assertExists(laptop);
  assertEquals(laptop.price, 999.99);
  
  // Update the document
  const updateResult = await products.updateOne(
    { name: "Laptop" },
    { $set: { price: 899.99 } }
  );
  
  // Verify update result
  assertExists(updateResult);
  assertEquals(updateResult.acknowledged, true);
  assertEquals(updateResult.matchedCount, 1);
  assertEquals(updateResult.modifiedCount, 1);
  
  // Delete the document
  const deleteResult = await products.deleteOne({ name: "Laptop" });
  
  // Verify delete result
  assertExists(deleteResult);
  assertEquals(deleteResult.acknowledged, true);
  assertEquals(deleteResult.deletedCount, 1);
});

// Run the test
console.log("Running MQLib PostgreSQL tests..."); 