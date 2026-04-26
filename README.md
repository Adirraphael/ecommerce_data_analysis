# Ecommerce Sales Analysis                                        

A personal project to build an end-to-end sales analytics pipeline using modern data stack tools — from synthetic data generation to a fully interactive Tableau dashboard. I built a scalable ELT pipeline using Snowflake as the cloud data warehouse, dbt for dimensional modeling and data transformation, and GitHub Actions for workflow automation.

The pipeline ingests synthetic ecommerce data, executes incremental loads, applies business logic through staging and marts layers, and surfaces insights via interactive Tableau dashboards with cross-filtering capabilities.                                                                          
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
                  
