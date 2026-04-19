with customers as (                             
      select * from {{ ref('stg_customers') }}
  ),                                              
   
  orders as (                                     
      select      
          customer_id,
          count(order_id)        as total_orders,
          sum(total_amount)      as total_spent,
          min(order_date)        as               
  first_order_date,
          max(order_date)        as               
  last_order_date 
      from {{ ref('stg_orders') }}
      group by customer_id
  ),

  joined as (
      select
          c.customer_id,
          c.full_name,                            
          c.first_name,
          c.last_name,                            
          c.email,
          c.city,
          c.state,
          c.country,
          c.signup_date,
          o.total_orders,
          o.total_spent,                          
          o.first_order_date,
          o.last_order_date                       
      from customers c
      left join orders o on c.customer_id =
  o.customer_id
  )

  select * from joined