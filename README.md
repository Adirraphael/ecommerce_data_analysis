# Ecommerce Sales Analysis                                         
  A personal project to build an end-to-end sales analytics pipeline 
  using modern data stack tools.

  ## What this project does

  I generated a synthetic ecommerce dataset and loaded it into       
  Snowflake, then built a dbt data model on top of it, and plan to
  visualize everything in Tableau.                                   
                  
  ## Tech Stack

  - **Python** — generated and loaded fake ecommerce data into       
  Snowflake
  - **Snowflake** — cloud data warehouse storing raw and transformed 
  data                                                               
  - **dbt** — data transformation and modeling
  - **Tableau** — dashboarding and visualization (coming soon)       
                  
  ## Data Pipeline

  Raw data (Python) → Snowflake (ECOMMERCE_RAW) → dbt staging → dbt  
  marts → Tableau
                                                                     
  ## Data Model   
                                                                     
  **Staging layer** — cleans and standardizes the 4 raw tables:
  - `stg_customers` — customer info with full_name added             
  - `stg_products` — products with margin and margin_pct calculated  
  - `stg_orders` — orders with order_date cast as date               
  - `stg_order_items` — order line items                             
                                                                     
  **Marts layer** — business-facing tables:
  - `fct_orders` — one row per order                                 
  - `fct_order_items` — one row per order line item with product
  details                                                            
  - `dim_customers` — customers with aggregated order stats
  - `dim_products` — product catalog with pricing and margin         
                  
