/**
 * Collection interface for MQLib
 */

import { Document, WithId, OptionalId } from "../types/document.ts";
import { Filter, FindOptions } from "../types/filter.ts";
import { UpdateOperator, UpdateOptions, DeleteOptions } from "../types/update.ts";
import { 
  InsertOneResult, 
  InsertManyResult, 
  UpdateResult, 
  DeleteResult, 
  CountResult,
  IndexInfo
} from "../types/results.ts";

/**
 * Options for index creation.
 */
export interface IndexOptions {
  /** Whether the index should enforce uniqueness */
  unique?: boolean;
  /** Whether the index should only include documents that contain the indexed field */
  sparse?: boolean;
  /** Custom name for the index */
  name?: string;
}

/**
 * Represents a collection of documents in the database.
 * 
 * A collection is a group of documents that share a common structure,
 * similar to a table in a relational database.
 * 
 * @template T The document type stored in this collection
 */
export interface Collection<T extends Document> {
  /**
   * Gets the name of the collection.
   */
  readonly name: string;

  /**
   * Finds a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to the matching document, or null if none is found
   */
  findOne(filter: Filter<T>, options?: FindOptions<T>): Promise<WithId<T> | null>;

  /**
   * Finds all documents that match the filter.
   * 
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to an array of matching documents
   */
  find(filter: Filter<T>, options?: FindOptions<T>): Promise<WithId<T>[]>;

  /**
   * Inserts a single document into the collection.
   * 
   * @param doc The document to insert
   * @returns A promise that resolves to an InsertOneResult
   */
  insertOne(doc: OptionalId<T>): Promise<InsertOneResult>;

  /**
   * Inserts multiple documents into the collection.
   * 
   * @param docs The documents to insert
   * @returns A promise that resolves to an InsertManyResult
   */
  insertMany(docs: OptionalId<T>[]): Promise<InsertManyResult>;

  /**
   * Updates a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  updateOne(filter: Filter<T>, update: UpdateOperator<T>, options?: UpdateOptions): Promise<UpdateResult<T>>;

  /**
   * Updates all documents that match the filter.
   * 
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  updateMany(filter: Filter<T>, update: UpdateOperator<T>, options?: UpdateOptions): Promise<UpdateResult<T>>;

  /**
   * Deletes a single document that matches the filter.
   * 
   * @param filter The query filter
   * @param options Options for the delete operation
   * @returns A promise that resolves to a DeleteResult
   */
  deleteOne(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult>;

  /**
   * Deletes all documents that match the filter.
   * 
   * @param filter The query filter
   * @param options Options for the delete operation
   * @returns A promise that resolves to a DeleteResult
   */
  deleteMany(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult>;

  /**
   * Counts the number of documents that match the filter.
   * 
   * @param filter The query filter
   * @returns A promise that resolves to the count
   */
  countDocuments(filter?: Filter<T>): Promise<number>;

  /**
   * Creates an index on the specified field(s).
   * 
   * @param fieldOrSpec The field name or index specification
   * @param options Options for the index
   * @returns A promise that resolves to the name of the created index
   */
  createIndex(fieldOrSpec: string | Record<string, 1 | -1>, options?: IndexOptions): Promise<string>;

  /**
   * Lists all indexes on the collection.
   * 
   * @returns A promise that resolves to an array of index information
   */
  listIndexes(): Promise<IndexInfo[]>;

  /**
   * Drops an index from the collection.
   * 
   * @param indexName The name of the index to drop
   * @returns A promise that resolves when the index is dropped
   */
  dropIndex(indexName: string): Promise<void>;
} 