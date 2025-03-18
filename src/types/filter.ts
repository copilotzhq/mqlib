/**
 * Filter type definitions for MQLib query operations
 */

/**
 * Comparison operators for query filters.
 * 
 * @template T The type of the field being compared
 */
export type ComparisonOperator<T> = {
  /** Matches values equal to the specified value */
  $eq?: T;
  /** Matches values greater than the specified value */
  $gt?: T;
  /** Matches values greater than or equal to the specified value */
  $gte?: T;
  /** Matches values less than the specified value */
  $lt?: T;
  /** Matches values less than or equal to the specified value */
  $lte?: T;
  /** Matches values not equal to the specified value */
  $ne?: T;
  /** Matches values in the specified array */
  $in?: T[];
  /** Matches values not in the specified array */
  $nin?: T[];
};

/**
 * Logical operators for query filters.
 * 
 * @template T The document type being filtered
 */
export type LogicalOperator<T> = {
  /** Joins query clauses with a logical AND */
  $and?: Filter<T>[];
  /** Joins query clauses with a logical OR */
  $or?: Filter<T>[];
  /** Joins query clauses with a logical NOR */
  $nor?: Filter<T>[];
  /** Inverts the effect of a query expression */
  $not?: Filter<T>;
};

/**
 * Array operators for query filters.
 * 
 * @template T The type of the array field
 */
export type ArrayOperator<T> = {
  /** Matches arrays that contain all specified elements */
  $all?: T[];
  /** Matches arrays that contain at least one element matching all the specified conditions */
  $elemMatch?: Filter<T>;
  /** Matches arrays with the specified number of elements */
  $size?: number;
};

/**
 * Element operators for query filters.
 */
export type ElementOperator = {
  /** Matches documents that have the specified field */
  $exists?: boolean;
  /** Matches documents where the value of a field is of the specified type */
  $type?: string;
};

/**
 * Evaluation operators for query filters.
 */
export type EvaluationOperator = {
  /** Matches values that satisfy a regular expression */
  $regex?: string | RegExp;
  /** Matches values where the remainder of division by a specified number equals a specified value */
  $mod?: [number, number];
};

/**
 * Represents a MongoDB-style query filter.
 * 
 * Filters can include direct field comparisons, comparison operators,
 * array operators, element operators, and logical operators.
 * 
 * @example
 * ```ts
 * // Simple equality filter
 * const filter: Filter<User> = { name: "John" };
 * 
 * // Comparison operator
 * const filter: Filter<User> = { age: { $gt: 25 } };
 * 
 * // Logical operator
 * const filter: Filter<User> = {
 *   $or: [
 *     { name: "John" },
 *     { name: "Jane" }
 *   ]
 * };
 * 
 * // Array operator
 * const filter: Filter<User> = { tags: { $all: ["developer", "deno"] } };
 * ```
 * 
 * @template T The document type to filter
 */
export type Filter<T = any> =
  & {
    [P in keyof T & string]?:
      | T[P]
      | ComparisonOperator<T[P]>
      | ArrayOperator<T[P]>
      | ElementOperator
      | EvaluationOperator;
  }
  & LogicalOperator<T>;

/**
 * Options for find operations.
 * 
 * @template T The document type being queried
 */
export interface FindOptions<T = any> {
  /** Sorting criteria (field name to sort direction mapping) */
  sort?: Record<string, 1 | -1>;
  /** Maximum number of documents to return */
  limit?: number;
  /** Number of documents to skip */
  skip?: number;
  /** Fields to include or exclude in the result */
  projection?: Record<string, 0 | 1 | boolean>;
} 