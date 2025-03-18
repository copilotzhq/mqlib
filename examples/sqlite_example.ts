/**
 * Example usage of MQLib with SQLite
 */

import { Database } from "../src/core/database.ts";
import { SqliteAdapter } from "../src/adapters/sqlite/sqlite_adapter.ts";
import { Document } from "../src/types/document.ts";

// For this example, we'll use a mock connection
// In a real application, you would use a real SQLite connection
const mockConnection = {
  query: async (sql: string, params: unknown[] = []) => {
    console.log("Executing SQL:", sql);
    console.log("Parameters:", params);
    
    // Mock response for different query types
    if (sql.startsWith("SELECT COUNT")) {
      return { rows: [{ count: 0 }] };
    }
    
    if (sql.startsWith("SELECT")) {
      return { rows: [] };
    }
    
    return { rowCount: 0 };
  }
};

// Define a User interface
interface User extends Document {
  name: string;
  email: string;
  age: number;
  tags: string[];
}

async function main() {
  // Create a SQLite adapter
  const adapter = new SqliteAdapter();
  
  // Create a database instance
  const db = new Database("example", adapter, mockConnection);
  
  // Define a schema for the users collection
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
  
  console.log("Creating users collection...");
  await db.createCollection<User>("users", userSchema);
  
  // Get a reference to the users collection
  const users = db.collection<User>("users");
  
  console.log("Inserting a user...");
  await users.insertOne({
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    tags: ["developer", "deno"]
  });
  
  console.log("Finding users with age < 25...");
  const youngUsers = await users.find({ age: { $lt: 25 } });
  console.log("Young users:", youngUsers);
  
  console.log("Updating a user...");
  await users.updateOne(
    { email: "john@example.com" },
    { $set: { age: 31 }, $push: { tags: "updated" } }
  );
  
  console.log("Finding users with complex conditions...");
  const result = await users.find({
    $or: [
      { age: { $lt: 20 } },
      { age: { $gt: 60 } }
    ],
    $and: [
      { tags: { $in: ["developer", "designer"] } },
      { email: { $regex: "@example.com$" } }
    ]
  });
  console.log("Result:", result);
  
  console.log("Creating an index...");
  await users.createIndex("email", { unique: true });
  
  console.log("Deleting a user...");
  await users.deleteOne({ email: "john@example.com" });
}

// Run the example
main().catch(console.error); 