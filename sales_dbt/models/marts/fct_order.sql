with orders as (
      select * from {{ ref('stg_orders') }}
  )                                               
   
  select                                          
      order_id,   
      customer_id,
      order_date,
      status,
      total_amount
  from orders