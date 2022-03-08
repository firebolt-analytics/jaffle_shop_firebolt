# Changelog

## v1.0.1

### Changes

- Added join and aggregate indexes for testing.

### Breaking changes

- Changed field name in model index configs from `type` to `index_type`. This will cause errors when using older models with indexes.

## v1.0.0

- Jaffle Shop Firebolt now supports dbt 1.0!

## Under the hood

- `source-paths` and `data-paths` were changed to `model-paths` and `seed-paths`, respectively, in `dbt-project.yml`.

## v.0.21.9

- Jaffle Shop Firebolt now supports dbt-firebolt v0.21.9, which uses firebolt-sdk.

### Under the hood

- Prehook for SET commands was removed from `dbt_project.yml`.

## v.0.21.0

### Fixes

- Many

### Under the Hood

- A lot of work!
- Supports `dbt-firebolt` versions 0.21.0–0.21.8.
