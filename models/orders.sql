{{
  config(
    materialized = 'incremental',
    table_type = 'fact',
    primary_index = ['customer_id'],
    indexes = [
      {
        'index_type': 'aggregating',
        'key_columns': ['customer_id', 'order_id'],
        'aggregation': ['SUM(credit_card_amount)', 'SUM(amount)']
      }
    ],
    incremental_strategy = 'append',
    partition_by = 'status'
  )
}}

{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

WITH orders AS (
  SELECT * FROM {{ ref('stg_orders') }}
),
payments AS (
  SELECT * FROM {{ ref('stg_payments') }}
),
order_payments AS (
  SELECT
    order_id,
    {% for payment_method in payment_methods -%}
    SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN amount ELSE 0 END) AS {{ payment_method }}_amount,
    {% endfor -%}
    SUM(amount) AS total_amount
  FROM payments
  GROUP BY order_id
),
final as (
  SELECT
      orders.order_id,
      orders.customer_id,
      orders.order_date,
      orders.status,
      {% for payment_method in payment_methods -%}
          order_payments.{{ payment_method }}_amount,
      {% endfor -%}
      order_payments.total_amount AS amount
  FROM orders
       LEFT JOIN order_payments
       ON orders.order_id = order_payments.order_id
)

SELECT * FROM final
