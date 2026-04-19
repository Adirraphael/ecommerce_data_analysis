with source as (                                
      select * from {{ source('ecommerce_raw',    
  'order_items') }}                               
  ),              

  renamed as (
      select
          order_item_id,
          order_id,
          product_id,
          quantity,                               
          unit_price,
          subtotal                                
      from source 
  )

  select * from renamed