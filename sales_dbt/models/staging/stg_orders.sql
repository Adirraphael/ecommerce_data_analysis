with source as (                                
      select * from {{ source('ecommerce_raw',
  'orders') }}                                    
  ),              

  renamed as (
      select
          order_id,
          customer_id,
          order_date::date as order_date,         
          status,
          total_amount                            
      from source 
  )

  select * from renamed