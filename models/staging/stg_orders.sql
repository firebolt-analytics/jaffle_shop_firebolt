{{
  config(
    table_type = 'incremental',
    primary_index = 'order_id',
    materialized = 'ephemeral',
    incremental_strategy = 'insert_overwrite',
    partition_by = ['order_date'],
   )
}}


WITH source AS (
  {#-
  Normally we would select from the table here, but we are using seeds to load
  our data in this project
  -#}
  SELECT * FROM {{ ref('raw_orders') }}
  {%- if is_incremental() %}
     WHERE order_date > (SELECT CAST(MAX(first_order) AS DATE)-13 FROM {{ this }})
  {%- endif %}
),
renamed AS (
  SELECT
      id AS order_id,
      user_id AS customer_id,
      order_date,
      status
  FROM source
)
SELECT * FROM renamed
