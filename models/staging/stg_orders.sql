{{
  config(
    table_type = 'dimension',
    primary_index = 'order_id',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = ['customer_id', 'status']
    )
}}


with source as (
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}
    {% if is_incremental() %}
       where order_date > (select cast(max(order_date) AS DATE)-3 from {{ this }})
    {% endif %}
),
renamed as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from source
)
select * from renamed
