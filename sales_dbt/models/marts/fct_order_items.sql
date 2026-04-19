with order_items as (                           
      select * from {{ ref('stg_order_items') }}  
  ),                                              
                                                  
  products as (                                   
      select * from {{ ref('stg_products') }}
  ),

  joined as (                                     
      select
          oi.order_item_id,                       
          oi.order_id,
          oi.product_id,
          oi.quantity,
          oi.unit_price,
          oi.subtotal,
          p.product_name,
          p.category,                             
          p.margin,
          p.margin_pct                            
      from order_items oi
      left join products p on oi.product_id =
  p.product_id
  )

  select * from joined