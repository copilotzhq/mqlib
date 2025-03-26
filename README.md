# MQLib - MongoDB Query Library for SQL Databases

MQLib is a library that provides a MongoDB-like API for SQL databases. It allows developers familiar with MongoDB's query language to work with SQL databases using the same syntax and operators they're already comfortable with.

## Features

- MongoDB-compatible collection API (`find`, `findOne`, `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `createIndex`)
- Support for MongoDB query operators (`$eq`, `$gt`, `$lt`, `$gte`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`, etc.)
- Support for MongoDB update operators (`$set`, `$unset`, `$inc`, `$push`, `$pull`, etc.)
- JSON Schema validation for document structure
- SQL dialect support (SQLite for now, PostgreSQL and MySQL coming soon)
- Dependency injection for database drivers (reduced bundle size)
- Parameterized queries to prevent SQL injection
- Efficient handling of nested objects (as JSON columns when appropriate)

## Installation

```bash
# Using Deno
import { Database, SqliteAdapter, createSqliteConnection } from "jsr:@copilotz/mqlib";
```

## Usage

### Connecting to a Database

MQLib uses dependency injection for database connections, which keeps the library small and gives you flexibility to choose your preferred database driver:

```typescript
import { Database, SqliteAdapter, createSqliteConnection } from "jsr:@copilotz/mqlib";

// Import your preferred SQLite library
// Native SQLite (requires --allow-env --allow-ffi --unstable-ffi)
import * as sqliteLib from "jsr:@db/sqlite@0.11";
// OR WASM SQLite
import * as sqliteLib from "jsr:@pomdtr/sqlite@3.9.1";

// Create a connection with auto-detection of library type
const connection = await createSqliteConnection(sqliteLib, "example.db");

// Create an adapter and database instance
const adapter = new SqliteAdapter();
const db = new Database("example", adapter, connection);
```

### Defining a Schema

```typescript
// Define a schema for a users collection
const userSchema = {
  type: "object",
  properties: {
    _id: { type: "string" },
    name: { type: "string" },
    email: { type: "string" },
    age: { type: "integer" },
    createdAt: { type: "string", format: "date-time" },
    tags: { type: "array", items: { type: "string" } }
  },
  required: ["_id", "name", "email"]
};

// Create the collection with the schema
await db.createCollection("users", userSchema);
```

### Working with Documents

```typescript
// Get a reference to the users collection
const users = db.collection("users");

// Insert a document
await users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  createdAt: new Date().toISOString(),
  tags: ["developer", "deno"]
});

// Find documents
const youngUsers = await users.find({ age: { $lt: 25 } });

// Update a document
await users.updateOne(
  { email: "john@example.com" },
  { $set: { age: 31 }, $push: { tags: "updated" } }
);

// Delete a document
await users.deleteOne({ email: "john@example.com" });
```

### Using Query Operators

```typescript
// Find users with age between 25 and 35
const middleAgedUsers = await users.find({
  age: { $gte: 25, $lte: 35 }
});

// Find users with specific tags
const developers = await users.find({
  tags: { $all: ["developer"] }
});

// Find users with complex conditions
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
```

### Creating Indexes

```typescript
// Create a simple index
await users.createIndex("email");

// Create a unique index
await users.createIndex("email", { unique: true });

// Create a compound index
await users.createIndex({ age: -1, name: 1 });

```


## SQL Dialect Support

MQLib supports multiple SQL dialects through adapters:

- SQLite: `SqliteAdapter`
- PostgreSQL: `PostgresAdapter` (coming soon)
- MySQL: `MySqlAdapter` (coming soon)

Each adapter handles the translation of MongoDB-style queries to the specific SQL dialect.

## License

MIT 