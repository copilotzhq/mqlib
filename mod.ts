/**
 * MQLib - MongoDB Query Library for SQL Databases
 * 
 * A library that provides a MongoDB-like API for SQL databases.
 * It allows developers familiar with MongoDB's query language to work with SQL databases
 * using the same syntax and operators they're already comfortable with.
 * 
 * Features:
 * - MongoDB-compatible collection API (find, insert, update, delete)
 * - Support for MongoDB query operators ($eq, $gt, $lt, etc.)
 * - Support for MongoDB update operators ($set, $inc, $push, etc.)
 * - JSON Schema validation
 * - SQL dialect support (SQLite, PostgreSQL, MySQL)
 * - Parameterized queries for SQL injection protection
 * - Efficient handling of nested objects and arrays
 */

// Export version information
export { VERSION, AUTHOR, LICENSE, getVersionInfo } from "./version.ts";

// Re-export all public APIs
export * from "./src/types/mod.ts";
export * from "./src/interfaces/mod.ts";
export * from "./src/core/mod.ts";
export * from "./src/adapters/mod.ts";

// Export main classes directly for convenience
export { Collection } from "./src/core/collection.ts";
export { Database } from "./src/core/database.ts";

// Export adapters directly for convenience
export { 
  SqliteAdapter, 
  createConnection as createSqliteConnection
} from "./src/adapters/sqlite/mod.ts";

// TO DO: Implement PostgreSQL and MySQL adapters
// export { PostgresAdapter, createPostgresConnection } from "./src/adapters/postgres/mod.ts";
// export { MySqlAdapter, createMySqlConnection } from "./src/adapters/mysql/mod.ts";
