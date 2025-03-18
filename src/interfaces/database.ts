/**
 * Database interface for MQLib
 */

import { Document } from "../types/document.ts";
import { Collection } from "./collection.ts";

/**
 * Represents a database in the system.
 * 
 * A database contains collections of documents.
 */
export interface Database {
  /**
   * Gets the name of the database.
   */
  readonly name: string;

  /**
   * Gets a collection with the specified name.
   * 
   * @template T The document type stored in the collection
   * @param name The name of the collection
   * @returns A Collection instance for the specified name
   */
  collection<T extends Document>(name: string): Collection<T>;

  /**
   * Lists all collections in the database.
   * 
   * @returns A promise that resolves to an array of collection names
   */
  listCollections(): Promise<string[]>;

  /**
   * Creates a collection with the specified name and schema.
   * 
   * @param name The name of the collection to create
   * @param schema The JSON schema for the collection (optional)
   * @returns A promise that resolves to the created collection
   */
  createCollection<T extends Document>(name: string, schema?: object): Promise<Collection<T>>;

  /**
   * Drops a collection from the database.
   * 
   * @param name The name of the collection to drop
   * @returns A promise that resolves when the collection is dropped
   */
  dropCollection(name: string): Promise<void>;
} 