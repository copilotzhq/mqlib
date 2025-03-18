/**
 * Document type definitions for MQLib
 */

/**
 * Represents a document ID.
 * This can be a string, number, or any other primitive type.
 */
export type DocumentId = string | number;

/**
 * Represents a document stored in a collection.
 * All documents must have an `_id` field.
 * Additional fields can be of any type and are defined by the generic type parameter.
 * 
 * @example
 * ```ts
 * interface User extends Document {
 *   name: string;
 *   email: string;
 *   age: number;
 * }
 * ```
 */
export interface Document {
  _id: DocumentId;
  [key: string]: unknown;
}

/**
 * Type that adds an _id field to a document type.
 * 
 * @template T The document type
 */
export type WithId<T> = T & { _id: DocumentId };

/**
 * Type that makes the _id field optional.
 * Used for document creation where _id can be auto-generated.
 * 
 * @template T The document type
 */
export type OptionalId<T> = Omit<T, "_id"> & { _id?: DocumentId }; 