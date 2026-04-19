with source as (                                          
      select * from {{ source('ecommerce_raw', 'customers') 
  }}                                                        
  ),              

  renamed as (
      select
          customer_id,
          first_name,                                       
          last_name,
          first_name || ' ' || last_name as full_name,      
          email,  
          city,
          state,
          country,
          signup_date::date as signup_date
      from source                                           
  )
                                                            
  select * from renamed