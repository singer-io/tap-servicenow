# Changelog

## Unreleased
  * Added field-level permission checks during discovery to exclude fields that lack read permissions
  * Discovery now tests each field individually and removes unauthorized fields from catalog
  * Prevents 403 "Field(s) present in the query do not have permission to be read" errors during sync
  * Tables with no accessible fields after permission filtering are excluded from catalog

## 0.0.1
  * Initial commit
