/**
 * Update operator type definitions for MQLib
 */

/**
 * Update operators for modifying documents.
 * 
 * @template T The document type being updated
 */
export type UpdateOperator<T> = {
  /** Sets the value of specified fields */
  $set?: Partial<T>;
  /** Removes specified fields */
  $unset?: Partial<Record<string, true>>;
  /** Increments the value of numeric fields */
  $inc?: Partial<Record<string, number>>;
  /** Multiplies the value of numeric fields */
  $mul?: Partial<Record<string, number>>;
  /** Updates fields with specified value if it's less than the current value */
  $min?: Partial<Record<string, number | Date>>;
  /** Updates fields with specified value if it's greater than the current value */
  $max?: Partial<Record<string, number | Date>>;
  /** Adds elements to array fields */
  $push?: Partial<Record<string, unknown>>;
  /** Removes elements from array fields that match a condition */
  $pull?: Partial<Record<string, unknown>>;
  /** Adds elements to array fields only if they don't already exist */
  $addToSet?: Partial<Record<string, unknown>>;
  /** Sets the value of a field if the document is inserted during an upsert */
  $setOnInsert?: Partial<T>;
};

/**
 * Options for update operations.
 * 
 * @template T The document type being updated
 */
export interface UpdateOptions<T = any> {
  /**
   * Whether to insert a document if no documents match the filter.
   * Default is false.
   */
  upsert?: boolean;
  /**
   * Array filters for updating elements in arrays.
   * Used with positional operators in update expressions.
   */
  arrayFilters?: Array<Record<string, any>>;
}

/**
 * Options for delete operations.
 */
export interface DeleteOptions {
  // Future options can be added here
} 