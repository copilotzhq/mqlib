/**
 * MQLib MySQL Example
 * 
 * This example demonstrates how to use MQLib with MySQL.
 * 
 * Prerequisites:
 * - MySQL server running
 * - MySQL client for Deno
 */

// Import MQLib
import { Database } from "../src/core/database.ts";
import { MySqlAdapter } from "../src/adapters/mysql/mysql_adapter.ts";
import { Document } from "../src/types/document.ts";

// Define a document interface
interface Product extends Document {
  name: string;
  price: number;
  category: string;
  inStock: boolean;
  tags?: string[];
}

// Define a schema for the products collection
const productSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    price: { type: "number", minimum: 0 },
    category: { type: "string" },
    inStock: { type: "boolean" },
    tags: { 
      type: "array", 
      items: { type: "string" },
      nullable: true
    }
  },
  required: ["name", "price", "category", "inStock"]
};

async function main() {
  try {
    // In a real application, you would connect to MySQL:
    // const client = await new Client().connect({
    //   hostname: "127.0.0.1",
    //   username: "root",
    //   password: "password",
    //   db: "mqlib_demo",
    // });
    
    // For this example, we'll create a mock connection
    const client = {
      query: async (sql: string, params: unknown[] = []) => {
        console.log("SQL:", sql);
        console.log("Params:", params);
        return { rows: [], rowCount: 0 };
      }
    };

    console.log("Connected to MySQL");

    // Create MQLib database with MySQL adapter
    const db = new Database("mqlib_demo", new MySqlAdapter(), client);
    
    // Create products collection with schema
    const products = await db.createCollection<Product>("products", productSchema);
    console.log("Created products collection");
    
    // Insert a document
    const insertResult = await products.insertOne({
      name: "Laptop",
      price: 999.99,
      category: "Electronics",
      inStock: true,
      tags: ["computer", "portable"]
    });
    console.log("Inserted document with ID:", insertResult.insertedId);
    
    // Insert multiple documents
    const insertManyResult = await products.insertMany([
      {
        name: "Smartphone",
        price: 699.99,
        category: "Electronics",
        inStock: true,
        tags: ["mobile", "communication"]
      },
      {
        name: "Headphones",
        price: 149.99,
        category: "Audio",
        inStock: false
      }
    ]);
    console.log("Inserted multiple documents:", insertManyResult.insertedCount);
    
    // Find documents
    const allProducts = await products.find({});
    console.log("All products:", allProducts);
    
    // Find with query
    const electronics = await products.find({ category: "Electronics" });
    console.log("Electronics products:", electronics);
    
    // Find with operators
    const expensiveProducts = await products.find({ 
      price: { $gt: 500 } 
    });
    console.log("Expensive products:", expensiveProducts);
    
    // Update a document
    const updateResult = await products.updateOne(
      { name: "Headphones" },
      { $set: { inStock: true, price: 129.99 } }
    );
    console.log("Update result:", updateResult);
    
    // Find one and update
    const updatedProduct = await products.findOne({ name: "Headphones" });
    console.log("Updated product:", updatedProduct);
    
    // Count documents
    const count = await products.countDocuments({ inStock: true });
    console.log("Products in stock:", count);
    
    // Create an index
    await products.createIndex("category", { name: "category_idx" });
    console.log("Created index on category");
    
    // List collections
    const collections = await db.listCollections();
    console.log("Collections:", collections);
    
    // In a real application, you would close the connection:
    // await client.close();
    console.log("Example completed");
    
  } catch (error) {
    console.error("Error:", error);
  }
}

// Run the example
if (import.meta.main) {
  main();
} 