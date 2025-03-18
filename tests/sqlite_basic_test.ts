/**
 * Basic SQLite integration test for MQLib
 */

import { assertEquals, assertExists } from "https://deno.land/std@0.204.0/testing/asserts.ts";
import { Database } from "../src/core/database.ts";
import { SqliteAdapter } from "../src/adapters/sqlite/sqlite_adapter.ts";
import { Document } from "../src/types/document.ts";
import { createSqliteConnection } from "../src/adapters/sqlite/sqlite_connection.ts";

// Define a simple document interface
interface User extends Document {
  name: string;
  email: string;
  age: number;
}

// Define a simple schema
const userSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    email: { type: "string" },
    age: { type: "integer" }
  },
  required: ["_id", "name", "email"]
};

// Run the test
console.log("Running basic SQLite integration test...");

Deno.test("SQLite Basic Integration Test", async () => {
  // Create a connection using the factory
  const connection = await createSqliteConnection(":memory:");
  const adapter = new SqliteAdapter();
  const db = new Database("test", adapter, connection);
  
  try {
    // Create a collection
    const users = await db.createCollection<User>("users", userSchema);
    
    // Verify the collection was created
    assertExists(users);
    assertEquals(users.name, "users");
    
    // Insert a document
    const insertResult = await users.insertOne({
      name: "John Doe",
      email: "john@example.com",
      age: 30
    });
    
    // Verify the insert result
    assertExists(insertResult);
    assertExists(insertResult.insertedId);
    assertEquals(insertResult.acknowledged, true);
    console.log("Insert result:", insertResult);
    
    // List all documents to verify insertion
    const allUsers = await users.find({});
    console.log("All users:", allUsers);
    
    // Find the document
    const john = await users.findOne({ email: "john@example.com" });
    console.log("John:", john);
    assertExists(john);
    assertEquals(john.name, "John Doe");
    assertEquals(john.age, 30);
    
    // Update the document
    const updateResult = await users.updateOne(
      { email: "john@example.com" },
      { $set: { age: 31 } }
    );
    
    // Verify update result
    assertExists(updateResult);
    assertEquals(updateResult.acknowledged, true);
    assertEquals(updateResult.matchedCount, 1);
    assertEquals(updateResult.modifiedCount, 1);
    
    // Find the updated document
    const updatedJohn = await users.findOne({ email: "john@example.com" });
    assertExists(updatedJohn);
    assertEquals(updatedJohn.age, 31);
    
    // Delete the document
    const deleteResult = await users.deleteOne({ email: "john@example.com" });
    
    // Verify delete result
    assertExists(deleteResult);
    assertEquals(deleteResult.acknowledged, true);
    assertEquals(deleteResult.deletedCount, 1);
    
    // Verify the document was deleted
    const deletedJohn = await users.findOne({ email: "john@example.com" });
    assertEquals(deletedJohn, null);
    
  } finally {
    // Close the connection
    connection.close();
  }
}); 