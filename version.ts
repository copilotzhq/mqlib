/**
 * MQLib version information
 */

export const VERSION = "0.1.0";
export const AUTHOR = "MQLib Team";
export const LICENSE = "MIT";

/**
 * Returns the version information as a string
 */
export function getVersionInfo(): string {
  return `MQLib v${VERSION} - MongoDB Query Library for SQL Databases`;
}

// If this file is run directly, print the version info
if (import.meta.main) {
  console.log(getVersionInfo());
  console.log(`Author: ${AUTHOR}`);
  console.log(`License: ${LICENSE}`);
} 