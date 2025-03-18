/**
 * MQLib - All Dialects Example
 * 
 * This example demonstrates how to use MQLib with all three supported SQL dialects:
 * - SQLite
 * - PostgreSQL
 * - MySQL
 */

import { 
  Database, 
  SqliteAdapter, 
  PostgresAdapter, 
  MySqlAdapter,
  VERSION,
  getVersionInfo
} from "../mod.ts";
import { Document } from "../src/types/document.ts";

// Print version information
console.log(getVersionInfo());
console.log("---");

// Define a document interface
interface Product extends Document {
  name: string;
  price: number;
  category: string;
  inStock: boolean;
}

// Define a schema for the products collection
const productSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    price: { type: "number", minimum: 0 },
    category: { type: "string" },
    inStock: { type: "boolean" }
  },
  required: ["name", "price", "category", "inStock"]
};

// Create mock connections for each database type
const createMockConnections = () => {
  // SQLite mock connection
  const sqliteConnection = {
    query: async (sql: string, params: unknown[] = []) => {
      console.log("[SQLite] SQL:", sql);
      console.log("[SQLite] Params:", params);
      return { rows: [], rowCount: 0 };
    }
  };

  // PostgreSQL mock connection
  const postgresConnection = {
    query: async (sql: string, params: unknown[] = []) => {
      console.log("[PostgreSQL] SQL:", sql);
      console.log("[PostgreSQL] Params:", params);
      return { rows: [], rowCount: 0 };
    }
  };

  // MySQL mock connection
  const mysqlConnection = {
    query: async (sql: string, params: unknown[] = []) => {
      console.log("[MySQL] SQL:", sql);
      console.log("[MySQL] Params:", params);
      return { rows: [], rowCount: 0 };
    }
  };

  return { sqliteConnection, postgresConnection, mysqlConnection };
};

async function main() {
  try {
    const { sqliteConnection, postgresConnection, mysqlConnection } = createMockConnections();

    // Create database instances for each dialect
    const sqliteDb = new Database("example", new SqliteAdapter(), sqliteConnection);
    const postgresDb = new Database("example", new PostgresAdapter(), postgresConnection);
    const mysqlDb = new Database("example", new MySqlAdapter(), mysqlConnection);

    console.log("=== Creating Collections ===");
    // Create collections with the same schema in each database
    const sqliteProducts = await sqliteDb.createCollection<Product>("products", productSchema);
    const postgresProducts = await postgresDb.createCollection<Product>("products", productSchema);
    const mysqlProducts = await mysqlDb.createCollection<Product>("products", productSchema);

    console.log("\n=== Inserting Documents ===");
    // Insert the same document in each database
    const product = {
      name: "Laptop",
      price: 999.99,
      category: "Electronics",
      inStock: true
    };

    await sqliteProducts.insertOne(product);
    await postgresProducts.insertOne(product);
    await mysqlProducts.insertOne(product);

    console.log("\n=== Querying Documents ===");
    // Query with the same filter in each database
    const filter = { category: "Electronics", price: { $gt: 500 } };
    
    await sqliteProducts.find(filter);
    await postgresProducts.find(filter);
    await mysqlProducts.find(filter);

    console.log("\n=== Updating Documents ===");
    // Update with the same update operator in each database
    const update = { $set: { price: 899.99, inStock: false } };
    
    await sqliteProducts.updateOne({ name: "Laptop" }, update);
    await postgresProducts.updateOne({ name: "Laptop" }, update);
    await mysqlProducts.updateOne({ name: "Laptop" }, update);

    console.log("\n=== Creating Indexes ===");
    // Create the same index in each database
    await sqliteProducts.createIndex("category");
    await postgresProducts.createIndex("category");
    await mysqlProducts.createIndex("category");

    console.log("\n=== Deleting Documents ===");
    // Delete with the same filter in each database
    await sqliteProducts.deleteOne({ name: "Laptop" });
    await postgresProducts.deleteOne({ name: "Laptop" });
    await mysqlProducts.deleteOne({ name: "Laptop" });

  } catch (error) {
    console.error("Error:", error);
  }
}

// Run the example
if (import.meta.main) {
  main();
} 