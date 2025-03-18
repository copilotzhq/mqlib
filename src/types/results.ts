/**
 * Result type definitions for MQLib operations
 */

import { DocumentId } from "./document.ts";

/**
 * Result of an insertOne operation.
 */
export interface InsertOneResult {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** The ID of the inserted document */
  insertedId: DocumentId;
}

/**
 * Result of an insertMany operation.
 */
export interface InsertManyResult {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** Number of documents that were inserted */
  insertedCount: number;
  /** Array of IDs for the inserted documents */
  insertedIds: DocumentId[];
  /** Whether the operation encountered write errors */
  hasWriteErrors: boolean;
  /** Details of any write errors that occurred */
  writeErrors?: Array<{ index: number; error: Error }>;
}

/**
 * Represents the result of an update operation.
 * 
 * @template T The document type that was updated
 */
export interface UpdateResult<T = any> {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** Number of documents that matched the filter */
  matchedCount: number;
  /** Number of documents that were modified */
  modifiedCount: number;
  /** The ID of the document that was upserted, or null if no upsert occurred */
  upsertedId: DocumentId | null;
  /** Number of documents that were upserted (0 or 1) */
  upsertedCount: number;
}

/**
 * Result of a delete operation.
 */
export interface DeleteResult {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** Number of documents that were deleted */
  deletedCount: number;
}

/**
 * Result of a count operation.
 */
export interface CountResult {
  /** Number of documents that matched the filter */
  count: number;
}

/**
 * Information about an existing index.
 */
export interface IndexInfo {
  /** Name of the index */
  name: string;
  /** Fields included in the index and their sort direction */
  key: Record<string, 1 | -1>;
  /** Whether the index enforces uniqueness */
  unique?: boolean;
  /** Whether the index only includes documents that contain the indexed field */
  sparse?: boolean;
} 