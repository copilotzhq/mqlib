/**
 * Comprehensive example of MQLib with PostgreSQL
 * 
 * This example demonstrates:
 * - Connection setup
 * - Schema definition and validation
 * - CRUD operations
 * - Query operators
 * - Update operators
 * - Indexing
 * - Transactions
 */

import { Database } from "../src/core/database.ts";
import { PostgresAdapter } from "../src/adapters/postgres/postgres_adapter.ts"; // Note: This adapter needs to be implemented
import { Document } from "../src/types/document.ts";

// In a real application, you would use a real PostgreSQL client
// For this example, we'll create a mock connection with more realistic behavior
const mockPgConnection = {
  query: async (sql: string, params: unknown[] = []) => {
    console.log("Executing SQL:", sql);
    console.log("Parameters:", params);
    
    // Mock response for different query types
    if (sql.includes("information_schema.tables")) {
      return { rows: [{ table_name: "products" }, { table_name: "users" }] };
    }
    
    if (sql.startsWith("SELECT COUNT")) {
      return { rows: [{ count: 5 }] };
    }
    
    if (sql.includes("FROM products")) {
      return { 
        rows: [
          { _id: "prod1", name: "Laptop", price: 999.99, category: "electronics", in_stock: true },
          { _id: "prod2", name: "Headphones", price: 149.99, category: "electronics", in_stock: true }
        ] 
      };
    }
    
    if (sql.includes("FROM users")) {
      return { 
        rows: [
          { _id: "user1", name: "Alice", email: "alice@example.com", age: 28 },
          { _id: "user2", name: "Bob", email: "bob@example.com", age: 34 }
        ] 
      };
    }
    
    return { rowCount: 1 };
  },
  
  // Mock transaction methods
  begin: async () => console.log("BEGIN TRANSACTION"),
  commit: async () => console.log("COMMIT"),
  rollback: async () => console.log("ROLLBACK")
};

// Define interfaces for our collections
interface Product extends Document {
  name: string;
  price: number;
  category: string;
  tags?: string[];
  in_stock: boolean;
  specs?: Record<string, unknown>;
}

interface User extends Document {
  name: string;
  email: string;
  age: number;
  address?: {
    street: string;
    city: string;
    country: string;
    postal_code: string;
  };
  preferences?: Record<string, unknown>;
}

async function main() {
  // Create a PostgreSQL adapter
  const adapter = new PostgresAdapter();
  
  // Create a database instance
  const db = new Database("ecommerce", adapter, mockPgConnection);
  
  // List all collections in the database
  console.log("Existing collections:");
  const collections = await db.listCollections();
  console.log(collections);
  
  // Define schemas
  const productSchema = {
    type: "object",
    properties: {
      _id: { type: "string" },
      name: { type: "string" },
      price: { type: "number", minimum: 0 },
      category: { type: "string" },
      tags: { type: "array", items: { type: "string" } },
      in_stock: { type: "boolean" },
      specs: { type: "object" }
    },
    required: ["_id", "name", "price", "category", "in_stock"]
  };
  
  const userSchema = {
    type: "object",
    properties: {
      _id: { type: "string" },
      name: { type: "string" },
      email: { type: "string", format: "email" },
      age: { type: "integer", minimum: 18 },
      address: {
        type: "object",
        properties: {
          street: { type: "string" },
          city: { type: "string" },
          country: { type: "string" },
          postal_code: { type: "string" }
        }
      },
      preferences: { type: "object" }
    },
    required: ["_id", "name", "email", "age"]
  };
  
  // Create collections with schemas
  console.log("Creating collections with schemas...");
  await db.createCollection<Product>("products", productSchema);
  await db.createCollection<User>("users", userSchema);
  
  // Get references to collections
  const products = db.collection<Product>("products");
  const users = db.collection<User>("users");
  
  // Create indexes
  console.log("Creating indexes...");
  await products.createIndex("category", { unique: false });
  await products.createIndex("price", { unique: false });
  await users.createIndex("email", { unique: true });
  
  // List indexes on the products collection
  try {
    console.log("Indexes on products collection:");
    const indexes = await products.listIndexes();
    console.log(indexes);
  } catch (error) {
    console.log("Note: listIndexes() requires a real PostgreSQL connection to work properly");
    console.log("With a mock connection, you would see:", [
      {
        name: "idx_products_category_asc",
        key: { category: 1 },
        unique: false
      },
      {
        name: "idx_products_price_asc",
        key: { price: 1 },
        unique: false
      }
    ]);
  }
  
  // Insert operations
  console.log("\n--- Insert Operations ---");
  
  // Insert a single product
  const insertResult = await products.insertOne({
    name: "Smartphone",
    price: 699.99,
    category: "electronics",
    tags: ["mobile", "android"],
    in_stock: true,
    specs: {
      screen: "6.5 inch",
      processor: "Octa-core",
      ram: "8GB"
    }
  });
  console.log("Inserted product:", insertResult);
  
  // Insert multiple users
  const usersToInsert = [
    {
      name: "Charlie",
      email: "charlie@example.com",
      age: 42,
      address: {
        street: "123 Main St",
        city: "Anytown",
        country: "USA",
        postal_code: "12345"
      }
    },
    {
      name: "Diana",
      email: "diana@example.com",
      age: 31,
      preferences: {
        theme: "dark",
        notifications: true
      }
    }
  ];
  
  const multiInsertResult = await users.insertMany(usersToInsert);
  console.log("Inserted users:", multiInsertResult);
  
  // Query operations
  console.log("\n--- Query Operations ---");
  
  // Find all products in the electronics category
  const electronics = await products.find({ category: "electronics" });
  console.log("Electronics products:", electronics);
  
  // Find products with price greater than 500
  const expensiveProducts = await products.find({ price: { $gt: 500 } });
  console.log("Expensive products:", expensiveProducts);
  
  // Find a specific user by email
  const alice = await users.findOne({ email: "alice@example.com" });
  console.log("User Alice:", alice);
  
  // Complex query with multiple operators
  const complexQuery = await products.find({
    category: "electronics",
    price: { $gte: 100, $lte: 1000 },
    tags: { $in: ["mobile", "wireless"] },
    $or: [
      { in_stock: true },
      { price: { $lt: 200 } }
    ]
  });
  console.log("Complex query results:", complexQuery);
  
  // Update operations
  console.log("\n--- Update Operations ---");
  
  // Update a single product
  const updateResult = await products.updateOne(
    { name: "Laptop" },
    { 
      $set: { price: 1099.99 },
      $push: { tags: "sale" }
    }
  );
  console.log("Update result:", updateResult);
  
  // Update multiple users
  const multiUpdateResult = await users.updateMany(
    { age: { $lt: 30 } },
    {
      $inc: { age: 1 },
      $set: { "preferences.notifications": false }
    }
  );
  console.log("Multi-update result:", multiUpdateResult);
  
  // Delete operations
  console.log("\n--- Delete Operations ---");
  
  // Delete a single product
  const deleteResult = await products.deleteOne({ name: "Headphones" });
  console.log("Delete result:", deleteResult);
  
  // Delete multiple users
  const multiDeleteResult = await users.deleteMany({ age: { $gt: 40 } });
  console.log("Multi-delete result:", multiDeleteResult);
  
  // Count documents
  console.log("\n--- Count Operations ---");
  
  const productCount = await products.countDocuments({ in_stock: true });
  console.log("In-stock products count:", productCount);
  
  const userCount = await users.countDocuments({ age: { $gte: 30 } });
  console.log("Users 30 or older count:", userCount);
  
  // Drop a collection
  console.log("\n--- Dropping Collection ---");
  await db.dropCollection("products");
  console.log("Products collection dropped");
}

// Run the example
main().catch(console.error); 