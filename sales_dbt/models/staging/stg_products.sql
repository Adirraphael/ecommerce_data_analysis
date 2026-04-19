with source as (
      select * from {{ source('ecommerce_raw', 'products') }}
  ),

  renamed as (
      select
          product_id,
          product_name,
          category,
          price,
          cost,
          round(price - cost, 2)              as margin,
          round((price - cost) / price * 100, 2) as margin_pct
      from source
  )

  select * from renamed