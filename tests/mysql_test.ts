/**
 * Test suite for MQLib using MySQL adapter
 * 
 * This file contains tests for the core functionality of MQLib
 * using the MySQL adapter with a mock connection.
 */

import { assertEquals, assertNotEquals, assertExists } from "https://deno.land/std@0.204.0/testing/asserts.ts";
import { Database } from "../src/core/database.ts";
import { MySqlAdapter } from "../src/adapters/mysql/mysql_adapter.ts";
import { Document, DocumentId } from "../src/types/document.ts";

// Create a mock MySQL connection that logs queries and returns mock data
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
        const tableName = sql.match(/CREATE TABLE IF NOT EXISTS `([^`]+)`/)?.[1];
        if (tableName) {
          tables[tableName] = [];
          return { rowCount: 0 };
        }
      }

      // Handle CREATE INDEX
      if (sql.startsWith("CREATE") && sql.includes("INDEX")) {
        const indexName = sql.match(/INDEX `([^`]+)`/)?.[1];
        const tableName = sql.match(/ON `([^`]+)`/)?.[1];
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
        const tableName = sql.match(/INSERT INTO `([^`]+)`/)?.[1];
        if (tableName && tables[tableName]) {
          // Generate an ID if not provided
          const doc: Record<string, any> = {};
          const columns = sql.match(/\(([^)]+)\)/)?.[1].split(", ").map(c => c.replace(/`/g, ""));
          
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
        const tableName = sql.match(/FROM `([^`]+)`/)?.[1];
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
        const tableName = sql.match(/FROM `([^`]+)`/)?.[1];
        if (tableName && tables[tableName]) {
          return { 
            rows: [{ count: tables[tableName].length }],
            rowCount: 1
          };
        }
      }

      // Handle UPDATE
      if (sql.startsWith("UPDATE")) {
        const tableName = sql.match(/UPDATE `([^`]+)`/)?.[1];
        if (tableName && tables[tableName]) {
          // Simple mock - just return that 1 row was updated
          return { rowCount: 1 };
        }
      }

      // Handle DELETE
      if (sql.startsWith("DELETE")) {
        const tableName = sql.match(/FROM `([^`]+)`/)?.[1];
        if (tableName && tables[tableName]) {
          // Simple mock - just return that 1 row was deleted
          return { rowCount: 1 };
        }
      }

      // Handle SHOW TABLES for listCollections
      if (sql === "SHOW TABLES") {
        return {
          rows: Object.keys(tables).map(name => ({ [name]: name })),
          rowCount: Object.keys(tables).length
        };
      }

      // Handle DROP TABLE
      if (sql.startsWith("DROP TABLE")) {
        const tableName = sql.match(/DROP TABLE IF EXISTS `([^`]+)`/)?.[1];
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
interface Order extends Document {
  customer: string;
  items: string[];
  total: number;
  status: string;
}

// Define a test schema
const orderSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    customer: { type: "string" },
    items: { type: "array", items: { type: "string" } },
    total: { type: "number", minimum: 0 },
    status: { type: "string" }
  },
  required: ["_id", "customer", "items", "total", "status"]
};

Deno.test("MQLib MySQL - Basic CRUD Operations", async () => {
  const connection = createMockConnection();
  const adapter = new MySqlAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const orders = await db.createCollection<Order>("orders", orderSchema);
  
  // Verify the collection was created
  assertExists(orders);
  assertEquals(orders.name, "orders");
  
  // Insert a document
  const insertResult = await orders.insertOne({
    customer: "John Doe",
    items: ["Product A", "Product B"],
    total: 99.99,
    status: "pending"
  });
  
  // Verify the insert result
  assertExists(insertResult);
  assertExists(insertResult.insertedId);
  assertEquals(insertResult.acknowledged, true);
  
  // Find the document
  const order = await orders.findOne({ customer: "John Doe" });
  assertExists(order);
  assertEquals(order.total, 99.99);
  assertEquals(order.status, "pending");
  
  // Update the document
  const updateResult = await orders.updateOne(
    { customer: "John Doe" },
    { $set: { status: "completed" } }
  );
  
  // Verify update result
  assertExists(updateResult);
  assertEquals(updateResult.acknowledged, true);
  assertEquals(updateResult.matchedCount, 1);
  assertEquals(updateResult.modifiedCount, 1);
  
  // Delete the document
  const deleteResult = await orders.deleteOne({ customer: "John Doe" });
  
  // Verify delete result
  assertExists(deleteResult);
  assertEquals(deleteResult.acknowledged, true);
  assertEquals(deleteResult.deletedCount, 1);
});

// Run the test
console.log("Running MQLib MySQL tests..."); 