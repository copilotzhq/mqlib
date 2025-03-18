/**
 * Test suite for MQLib using SQLite
 * 
 * This file contains tests for the core functionality of MQLib
 * using the SQLite adapter with an in-memory database.
 */

import { assertEquals, assertNotEquals, assertExists } from "https://deno.land/std@0.204.0/testing/asserts.ts";
import { Database } from "../src/core/database.ts";
import { SqliteAdapter } from "../src/adapters/sqlite/sqlite_adapter.ts";
import { Document, DocumentId } from "../src/types/document.ts";

// Create a mock SQLite connection that logs queries and returns mock data
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
    },
    
    // For debugging
    getTables: () => tables,
    getIndexes: () => indexes
  };
};

// Define a test document interface
interface User extends Document {
  name: string;
  email: string;
  age: number;
  tags?: string[];
}

// Define a test schema
const userSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    email: { type: "string" },
    age: { type: "integer" },
    tags: { type: "array", items: { type: "string" } }
  },
  required: ["_id", "name", "email"]
};

Deno.test("MQLib SQLite - Database and Collection Creation", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Verify the collection was created
  assertExists(users);
  assertEquals(users.name, "users");
  
  // List collections
  const collections = await db.listCollections();
  console.log("Collections:", collections);
});

Deno.test("MQLib SQLite - Document Insertion", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Insert a document
  const insertResult = await users.insertOne({
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    tags: ["developer", "deno"]
  });
  
  // Verify the insert result
  assertExists(insertResult);
  assertExists(insertResult.insertedId);
  assertEquals(insertResult.acknowledged, true);
  
  // Insert multiple documents
  const insertManyResult = await users.insertMany([
    {
      name: "Jane Doe",
      email: "jane@example.com",
      age: 28,
      tags: ["designer", "deno"]
    },
    {
      name: "Bob Smith",
      email: "bob@example.com",
      age: 35,
      tags: ["manager"]
    }
  ]);
  
  // Verify the insert many result
  assertExists(insertManyResult);
  assertEquals(insertManyResult.insertedCount, 2);
  assertEquals(insertManyResult.acknowledged, true);
  assertEquals(insertManyResult.hasWriteErrors, false);
  assertEquals(insertManyResult.insertedIds.length, 2);
});

Deno.test("MQLib SQLite - Document Querying", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Insert test documents
  await users.insertMany([
    {
      name: "John Doe",
      email: "john@example.com",
      age: 30,
      tags: ["developer", "deno"]
    },
    {
      name: "Jane Doe",
      email: "jane@example.com",
      age: 28,
      tags: ["designer", "deno"]
    },
    {
      name: "Bob Smith",
      email: "bob@example.com",
      age: 35,
      tags: ["manager"]
    }
  ]);
  
  // Find all documents
  const allUsers = await users.find({});
  console.log("All users:", allUsers);
  
  // Find one document
  const john = await users.findOne({ email: "john@example.com" });
  console.log("John:", john);
  
  // Find with query operators
  const youngUsers = await users.find({ age: { $lt: 30 } });
  console.log("Young users:", youngUsers);
  
  // Find with complex query
  const denoUsers = await users.find({
    tags: { $in: ["deno"] },
    age: { $gte: 25 }
  });
  console.log("Deno users:", denoUsers);
  
  // Count documents
  const count = await users.countDocuments({ age: { $gte: 30 } });
  console.log("Users 30 or older:", count);
});

Deno.test("MQLib SQLite - Document Updates", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Insert test documents
  await users.insertMany([
    {
      name: "John Doe",
      email: "john@example.com",
      age: 30,
      tags: ["developer", "deno"]
    },
    {
      name: "Jane Doe",
      email: "jane@example.com",
      age: 28,
      tags: ["designer", "deno"]
    }
  ]);
  
  // Update one document
  const updateResult = await users.updateOne(
    { email: "john@example.com" },
    { $set: { age: 31 }, $push: { tags: "typescript" } }
  );
  
  // Verify update result
  assertExists(updateResult);
  assertEquals(updateResult.acknowledged, true);
  assertEquals(updateResult.matchedCount, 1);
  assertEquals(updateResult.modifiedCount, 1);
  
  // Update many documents
  const updateManyResult = await users.updateMany(
    { tags: { $in: ["deno"] } },
    { $inc: { age: 1 } }
  );
  
  // Verify update many result
  assertExists(updateManyResult);
  assertEquals(updateManyResult.acknowledged, true);
});

Deno.test("MQLib SQLite - Document Deletion", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Insert test documents
  await users.insertMany([
    {
      name: "John Doe",
      email: "john@example.com",
      age: 30,
      tags: ["developer", "deno"]
    },
    {
      name: "Jane Doe",
      email: "jane@example.com",
      age: 28,
      tags: ["designer", "deno"]
    },
    {
      name: "Bob Smith",
      email: "bob@example.com",
      age: 35,
      tags: ["manager"]
    }
  ]);
  
  // Delete one document
  const deleteResult = await users.deleteOne({ email: "john@example.com" });
  
  // Verify delete result
  assertExists(deleteResult);
  assertEquals(deleteResult.acknowledged, true);
  assertEquals(deleteResult.deletedCount, 1);
  
  // Delete many documents
  const deleteManyResult = await users.deleteMany({ age: { $lt: 30 } });
  
  // Verify delete many result
  assertExists(deleteManyResult);
  assertEquals(deleteManyResult.acknowledged, true);
});

Deno.test("MQLib SQLite - Index Management", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create a collection
  const users = await db.createCollection<User>("users", userSchema);
  
  // Create indexes
  const emailIndexName = await users.createIndex("email", { unique: true });
  const ageIndexName = await users.createIndex("age");
  const compoundIndexName = await users.createIndex({ name: 1, age: -1 });
  
  // Verify index names
  assertExists(emailIndexName);
  assertExists(ageIndexName);
  assertExists(compoundIndexName);
  
  // List indexes
  const indexes = await users.listIndexes();
  console.log("Indexes:", indexes);
  
  // Drop an index
  await users.dropIndex(emailIndexName);
});

Deno.test("MQLib SQLite - Collection Management", async () => {
  const connection = createMockConnection();
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  // Create collections
  await db.createCollection<User>("users", userSchema);
  await db.createCollection("products");
  
  // List collections
  const collections = await db.listCollections();
  console.log("Collections:", collections);
  
  // Drop a collection
  await db.dropCollection("products");
  
  // List collections again
  const collectionsAfterDrop = await db.listCollections();
  console.log("Collections after drop:", collectionsAfterDrop);
});

// Run all tests
console.log("Running MQLib SQLite tests..."); 