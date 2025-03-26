/**
 * Integration test for MQLib using SQLite
 * 
 * This file contains tests that use a real SQLite database
 * to verify the functionality of MQLib.
 * 
 * Note: This test requires the SQLite module for Deno.
 * Install with: deno install --allow-read --allow-write --allow-net https://deno.land/x/sqlite/mod.ts
 * 
 * For native SQLite tests, also requires:
 * --allow-env --allow-ffi --unstable-ffi flags
 */

import { assertEquals, assertNotEquals, assertExists } from "https://deno.land/std@0.204.0/testing/asserts.ts";
import { Database, type Document, SqliteAdapter, createSqliteConnection } from "../mod.ts";

// Define a test document interface with nested objects and arrays
interface User extends Document {
  name: string;
  email: string;
  age: number;
  tags?: string[];
  address?: {
    street: string;
    city: string;
    country: string;
    postalCode?: string;
  };
  preferences?: {
    theme: string;
    notifications: boolean;
    favorites: string[];
  };
  skills?: Array<{
    name: string;
    level: number;
    years: number;
  }>;
}

// Define a test schema with nested objects and arrays
const userSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    email: { type: "string" },
    age: { type: "integer" },
    tags: { type: "array", items: { type: "string" } },
    address: {
      type: "object",
      properties: {
        street: { type: "string" },
        city: { type: "string" },
        country: { type: "string" },
        postalCode: { type: "string" }
      }
    },
    preferences: {
      type: "object",
      properties: {
        theme: { type: "string" },
        notifications: { type: "boolean" },
        favorites: { type: "array", items: { type: "string" } }
      }
    },
    skills: {
      type: "array",
      items: {
        type: "object",
        properties: {
          name: { type: "string" },
          level: { type: "integer" },
          years: { type: "integer" }
        }
      }
    }
  },
  required: ["_id", "name", "email"]
};

// Function to run all tests with a specific SQLite implementation
async function runTestsWithImplementation(implementationType: "native" | "wasm") {
  const implName = implementationType === "native" ? "Native" : "WASM";
  
  // Import the appropriate SQLite implementation
  let sqliteLib;
  try {
    if (implementationType === "native") {
      sqliteLib = await import("jsr:@db/sqlite@0.11");
    } else {
      sqliteLib = await import("jsr:@pomdtr/sqlite@3.9.1");
    }
  } catch (error) {
    console.warn(`Failed to import ${implName} SQLite library:`, error);
    return; // Skip tests if the library is not available
  }
  
  Deno.test(`SQLite Integration (${implName}) - Basic CRUD Test`, async () => {
    // Create a connection using the factory with the imported library
    const connection = await createSqliteConnection(sqliteLib, ":memory:");
    console.log(`Using ${connection.getImplementationType()} SQLite implementation`);
    
    const adapter = new SqliteAdapter();
    
    // Enable the schema-based approach
    adapter.setSchemaEnabled(true);
    
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
      
      // Find all documents
      const allUsers = await users.find({});
      assertEquals(allUsers.length, 3);
      
      // Find one document
      const john = await users.findOne({ email: "john@example.com" });
      assertExists(john);
      assertEquals(john.name, "John Doe");
      
      // Update one document
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
      
      // Count documents
      const count = await users.countDocuments({ age: { $gte: 30 } });
      assertEquals(count, 2);
      
      // Delete one document
      const deleteResult = await users.deleteOne({ email: "bob@example.com" });
      
      // Verify delete result
      assertExists(deleteResult);
      assertEquals(deleteResult.acknowledged, true);
      assertEquals(deleteResult.deletedCount, 1);
      
      // Count documents after deletion
      const countAfterDelete = await users.countDocuments({});
      assertEquals(countAfterDelete, 2);
      
    } finally {
      // Close the connection
      try {
        connection.close();
      } catch (error) {
        // Ignore error if already closed
      }
    }
  });

  Deno.test(`SQLite Integration (${implName}) - Complex Objects and Queries`, async () => {
    // Create a connection using the factory with the imported library
    const connection = await createSqliteConnection(sqliteLib, ":memory:");
    
    const adapter = new SqliteAdapter();
    
    // Enable the schema-based approach
    adapter.setSchemaEnabled(true);
    
    const db = new Database("test", adapter, connection);
    
    try {
      // Create a collection
      const users = await db.createCollection<User>("users", userSchema);
      
      // Insert documents with nested objects and arrays
      await users.insertMany([
        {
          name: "John Doe",
          email: "john@example.com",
          age: 30,
          tags: ["developer", "deno", "typescript"],
          address: {
            street: "123 Main St",
            city: "San Francisco",
            country: "USA",
            postalCode: "94105"
          },
          preferences: {
            theme: "dark",
            notifications: true,
            favorites: ["coding", "reading", "hiking"]
          },
          skills: [
            { name: "JavaScript", level: 5, years: 7 },
            { name: "TypeScript", level: 4, years: 3 },
            { name: "Python", level: 3, years: 2 }
          ]
        },
        {
          name: "Jane Doe",
          email: "jane@example.com",
          age: 28,
          tags: ["designer", "deno", "ui/ux"],
          address: {
            street: "456 Market St",
            city: "New York",
            country: "USA",
            postalCode: "10001"
          },
          preferences: {
            theme: "light",
            notifications: false,
            favorites: ["design", "art", "travel"]
          },
          skills: [
            { name: "UI Design", level: 5, years: 5 },
            { name: "Figma", level: 5, years: 3 },
            { name: "CSS", level: 4, years: 6 }
          ]
        },
        {
          name: "Bob Smith",
          email: "bob@example.com",
          age: 35,
          tags: ["manager", "agile"],
          address: {
            street: "789 Broadway",
            city: "Chicago",
            country: "USA"
          },
          preferences: {
            theme: "system",
            notifications: true,
            favorites: ["management", "agile", "leadership"]
          },
          skills: [
            { name: "Project Management", level: 5, years: 10 },
            { name: "Agile", level: 4, years: 7 },
            { name: "Leadership", level: 5, years: 8 }
          ]
        }
      ]);
      
      // Test query with nested object field
      const sfUsers = await users.find({ "address.city": "San Francisco" });
      assertEquals(sfUsers.length, 1);
      assertEquals(sfUsers[0].name, "John Doe");
      
      // Test query with nested array field
      const jsDevs = await users.find({ "skills.name": "JavaScript" });
      assertEquals(jsDevs.length, 1);
      assertEquals(jsDevs[0].name, "John Doe");
      
      // Test query with array contains
      const denoUsers = await users.find({ tags: { $in: ["deno"] } });
      assertEquals(denoUsers.length, 2);
      
      // Test complex query with multiple conditions
      const complexQuery = await users.find({
        age: { $gte: 30 },
        tags: { $in: ["developer", "manager"] },
        "preferences.notifications": true
      });
      assertEquals(complexQuery.length, 2);
      
      // Test update with nested object
      await users.updateOne(
        { email: "john@example.com" },
        { 
          $set: { 
            "address.postalCode": "90001"
          } 
        }
      );
      
      const updatedJohn = await users.findOne({ email: "john@example.com" });
      
      assertExists(updatedJohn);
      assertExists(updatedJohn.address);
      assertEquals(updatedJohn.address.city, "San Francisco");
      assertEquals(updatedJohn.address.postalCode, "90001");
      
      // Test update with array operations
      await users.updateOne(
        { email: "jane@example.com" },
        { 
          $push: { tags: "accessibility" },
          $set: { "skills.0.level": 6 }
        }
      );
      
      const updatedJane = await users.findOne({ email: "jane@example.com" });
      assertExists(updatedJane);
      assertExists(updatedJane.tags);
      assertEquals(updatedJane.tags.includes("accessibility"), true);
      assertExists(updatedJane.skills?.[0]);
      assertExists(updatedJane.skills?.[0].level);
      
      // Test updating nested fields
      const bobBeforeUpdate = await users.findOne({ email: "bob@example.com" });
      
      await users.updateOne(
        { email: "bob@example.com" },
        {
          $set: {
            "preferences.theme": "dark",
            "address.postalCode": "60601"
          },
          $push: { tags: "mentor" },
          $inc: { age: 1 }
        }
      );
      
      const updatedBob = await users.findOne({ email: "bob@example.com" });
      
      assertExists(updatedBob);
      assertEquals(updatedBob.preferences?.theme, "dark");
      assertEquals(updatedBob.address?.postalCode, "60601");
      assertEquals(updatedBob.tags?.includes("mentor"), true);
      assertEquals(updatedBob.age, 36);
      
    } finally {
      // Close the connection
      try {
        connection.close();
      } catch (error) {
        // Ignore error if already closed
      }
    }
  });

  Deno.test(`SQLite Integration (${implName}) - Array Manipulation Operations`, async () => {
    // Create a connection using the factory with the imported library
    const connection = await createSqliteConnection(sqliteLib, ":memory:");
    
    const adapter = new SqliteAdapter();
    
    // Enable the schema-based approach
    adapter.setSchemaEnabled(true);
    
    const db = new Database("test", adapter, connection);
    
    try {
      // Create a collection
      const users = await db.createCollection<User>("users", userSchema);
      
      // Insert a user with arrays
      await users.insertOne({
        name: "Alice Johnson",
        email: "alice@example.com",
        age: 32,
        tags: ["developer", "mobile", "ios"],
        preferences: {
          theme: "light",
          notifications: true,
          favorites: ["swift", "kotlin"]
        },
        skills: [
          { name: "Swift", level: 5, years: 6 },
          { name: "Objective-C", level: 4, years: 8 }
        ]
      })
      await users.insertOne({
        name: "Bob Smith",
        email: "bob@example.com",
        age: 35,
        tags: ["developer", "android"],
        preferences: {
          theme: "dark",
          notifications: false,
          favorites: ["kotlin", "java"]
        },
        skills: [
          { name: "Kotlin", level: 1, years: 2 },
          { name: "Java", level: 2, years: 3 }
        ]
      });
      
      // Test array push
      let alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertExists(alice.tags);
      const initialTagsLength = alice.tags?.length || 0;
      
      await users.updateOne(
        { email: "alice@example.com" },
        { $push: { tags: "flutter" } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertExists(alice.tags);
      assertEquals(alice.tags?.length, initialTagsLength + 1);
      assertEquals(alice.tags?.[alice.tags.length - 1], "flutter");
      
      // Test $addToSet (add new value)
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      const tagsLengthBeforeAddToSet = alice.tags?.length || 0;
      
      await users.updateOne(
        { email: "alice@example.com" },
        { $addToSet: { tags: "react-native" } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertEquals(alice.tags?.length, tagsLengthBeforeAddToSet + 1);
      assertEquals(alice.tags?.includes("react-native"), true);
      
      // Test $addToSet (existing value - should not add)
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      const tagsLengthBeforeAddToSetExisting = alice.tags?.length || 0;
      
      await users.updateOne(
        { email: "alice@example.com" },
        { $addToSet: { tags: "flutter" } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertEquals(alice.tags?.length, tagsLengthBeforeAddToSetExisting);
      
      // Test $pull
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      const tagsLengthBeforePull = alice.tags?.length || 0;
      
      await users.updateOne(
        { email: "alice@example.com" },
        { $pull: { tags: "ios" } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertEquals(alice.tags?.length, tagsLengthBeforePull - 1);
      assertEquals(alice.tags?.includes("ios"), false);
      
      // Test pushing object to array
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertExists(alice.skills);
      const skillsLengthBeforePush = alice.skills?.length || 0;
      
      await users.updateOne(
        { email: "alice@example.com" },
        { $push: { skills: { name: "Flutter", level: 3, years: 1 } } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      
    
      assertExists(alice.skills);
      assertEquals(alice.skills?.length, skillsLengthBeforePush + 1);
      assertEquals(alice.skills?.[alice.skills.length - 1].name, "Flutter");
      
      // Test querying array with $elemMatch
      const experiencedDevs = await users.find({
        "skills": {
          $elemMatch: {
            "level": { $gte: 4 },
            "years": { $gte: 5 }
          }
        }
      });
      
      
      assertEquals(experiencedDevs.length, 1);
      assertEquals(experiencedDevs[0].email, "alice@example.com");
      
      // Test array update with $set on specific index
      await users.updateOne(
        { email: "alice@example.com" },
        { $set: { "skills.1.level": 5, "skills.1.years": 10 } }
      );
      
      alice = await users.findOne({ email: "alice@example.com" });
      assertExists(alice);
      assertExists(alice.skills);

      // test find by item in array
      const mobileDevelopers = await users.find({ "tags": { $in: ["mobile"] } });
      assertEquals(mobileDevelopers.length, 1);
      assertEquals(mobileDevelopers[0].email, "alice@example.com");
      assertEquals(mobileDevelopers[0].preferences?.theme, "light");
      
    } finally {
      // Close the connection
      try {
        connection.close();
      } catch (error) {
        // Ignore error if already closed
      }
    }
  });

  Deno.test(`SQLite Integration (${implName}) - Advanced Query Operations`, async () => {
    // Create a connection using the factory with the imported library
    const connection = await createSqliteConnection(sqliteLib, ":memory:");
    
    const adapter = new SqliteAdapter();
    
    // Enable the schema-based approach
    adapter.setSchemaEnabled(true);
    
    const db = new Database("test", adapter, connection);
    
    try {
      // Create a collection
      const users = await db.createCollection<User>("users", userSchema);
      
      // Insert multiple documents for testing
      await users.insertMany([
        {
          name: "Alice Johnson",
          email: "alice@example.com",
          age: 32,
          tags: ["developer", "mobile"],
          address: {
            city: "Seattle",
            country: "USA"
          }
        },
        {
          name: "Bob Williams",
          email: "bob@example.com",
          age: 45,
          tags: ["manager", "finance"],
          address: {
            city: "Boston",
            country: "USA"
          }
        },
        {
          name: "Charlie Brown",
          email: "charlie@example.com",
          age: 28,
          tags: ["developer", "web"],
          address: {
            city: "London",
            country: "UK"
          }
        },
        {
          name: "Diana Miller",
          email: "diana@example.com",
          age: 38,
          tags: ["designer", "mobile"],
          address: {
            city: "Paris",
            country: "France"
          }
        },
        {
          name: "Edward Davis",
          email: "edward@example.com",
          age: 24,
          tags: ["developer", "backend"],
          address: {
            city: "Berlin",
            country: "Germany"
          }
        }
      ]);
      
      // Test sorting - ascending order
      const ascendingAge = await users.find(
        {},
        { sort: { age: 1 } }
      );
      
      assertEquals(ascendingAge.length, 5);
      assertEquals(ascendingAge[0].name, "Edward Davis");
      assertEquals(ascendingAge[1].name, "Charlie Brown");
      assertEquals(ascendingAge[4].name, "Bob Williams");
      
      // Test sorting - descending order
      const descendingAge = await users.find(
        {},
        { sort: { age: -1 } }
      );
      
      assertEquals(descendingAge.length, 5);
      assertEquals(descendingAge[0].name, "Bob Williams");
      assertEquals(descendingAge[4].name, "Edward Davis");
      
      // Test sorting - multiple fields
      const multiFieldSort = await users.find(
        {},
        { sort: { "address.country": 1, age: -1 } }
      );
      
      assertEquals(multiFieldSort.length, 5);
      // First should be from France (alphabetically first)
      assertEquals(multiFieldSort[0].address?.country, "France");
      
      // Test pagination - skip
      const skipResults = await users.find(
        {},
        { skip: 2, sort: { age: 1 } }
      );
      
      assertEquals(skipResults.length, 3);
      assertEquals(skipResults[0].name, "Alice Johnson");
      
      // Test pagination - limit
      const limitResults = await users.find(
        {},
        { limit: 2, sort: { age: 1 } }
      );
      
      assertEquals(limitResults.length, 2);
      assertEquals(limitResults[0].name, "Edward Davis");
      assertEquals(limitResults[1].name, "Charlie Brown");
      
      // Test pagination - skip and limit combined
      const skipAndLimitResults = await users.find(
        {},
        { skip: 1, limit: 2, sort: { age: 1 } }
      );
      
      assertEquals(skipAndLimitResults.length, 2);
      assertEquals(skipAndLimitResults[0].name, "Charlie Brown");
      assertEquals(skipAndLimitResults[1].name, "Alice Johnson");
      
      // Test complex query with filter, projection, sort, and pagination
      const complexQueryResults = await users.find(
        { age: { $gte: 30 }, tags: { $in: ["developer", "designer"] } },
        { 
          sort: { age: -1 },
          limit: 2
        }
      );
      
      assertEquals(complexQueryResults.length, 2);
      assertEquals(complexQueryResults[0].name, "Diana Miller");
      assertEquals(complexQueryResults[1].name, "Alice Johnson");
      
    } finally {
      // Close the connection
      try {
        connection.close();
      } catch (error) {
        // Ignore error if already closed
      }
    }
  });
}

// Try running with each type of SQLite implementation
try {
  await runTestsWithImplementation("native");
} catch (error) {
  console.warn("Failed to run tests with native SQLite:", error);
}

try {
  await runTestsWithImplementation("wasm");
} catch (error) {
  console.warn("Failed to run tests with WASM SQLite:", error);
}

// Alternative approach: run with auto-detection to exercise that code path
Deno.test("SQLite Integration - Auto-detection Test", async () => {
  let connection;
  
  try {
    // Try to import both libraries and use whichever one works
    let sqliteLib;
    try {
      sqliteLib = await import("jsr:@db/sqlite@0.11");
    } catch {
      try {
        sqliteLib = await import("https://deno.land/x/sqlite@v3.9.1/mod.ts");
      } catch (error) {
        console.warn("Failed to import any SQLite library:", error);
        return; // Skip test if no library is available
      }
    }
    
    // Create a connection using the factory with auto-detection
    connection = await createSqliteConnection(sqliteLib, ":memory:");
    
    const adapter = new SqliteAdapter();
    adapter.setSchemaEnabled(true);
    
    const db = new Database("test", adapter, connection);
    
    // Simple test to verify auto-detection works
    const users = await db.createCollection<User>("users", userSchema);
    
    // Insert a document
    await users.insertOne({
      name: "Auto Test",
      email: "auto@example.com",
      age: 30
    });
    
    // Find the document
    const result = await users.findOne({ email: "auto@example.com" });
    assertExists(result);
    assertEquals(result.name, "Auto Test");
    
  } finally {
    // Close the connection
    if (connection) {
      try {
        connection.close();
      } catch (error) {
        // Ignore error if already closed
      }
    }
  }
}); 