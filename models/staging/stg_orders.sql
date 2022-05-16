{{
  config(
    table_type = 'dimension',
    primary_index = 'order_id',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partitions = [[89, "placed"], [41, 'placed'], [85, 'placed']]
  )
}}


WITH source AS (
  {#-
  Normally we would select from the table here, but we are using seeds to load
  our data in this project
  #}
  SELECT * FROM {{ ref('raw_orders') }}
  {% if is_incremental() %}
     WHERE order_date > (SELECT CAST(MAX(order_date) AS DATE)-3 FROM {{ this }})
  {% endif %}
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
