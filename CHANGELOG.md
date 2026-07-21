# Changelog

## Unreleased
  * Added field-level permission checks during discovery to exclude fields that lack read permissions
  * Optimized field permission checking using batch probing with divide-and-conquer algorithm
  * Best case: 1 API call per table (all fields accessible)
  * Worst case: log2(N) API calls per table (some fields unauthorized)
  * Field checks use query patterns matching sync to catch query-dependent permissions
  * Prevents 403 "Field(s) present in the query do not have permission to be read" errors during sync
  * Tables with no accessible fields after permission filtering are excluded from catalog

## 0.0.1
  * Initial commit
