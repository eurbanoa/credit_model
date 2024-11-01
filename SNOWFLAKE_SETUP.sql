-- Use the ACCOUNTADMIN role
use role accountadmin;

-- Create a virtual warehouse
create warehouse if not exists credit_risk_wh 
warehouse_type = 'STANDARD' 
warehouse_size = 'XSMALL' 
auto_suspend = 120 
auto_resume = TRUE 
max_cluster_count = 1 
min_cluster_count = 1;

-- Set the context to the CREDIT_RISK database and PUBLIC schema
use database credit_risk;
use schema public;
