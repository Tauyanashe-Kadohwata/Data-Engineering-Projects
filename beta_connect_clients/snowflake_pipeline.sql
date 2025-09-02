-- Create the task using a fully qualified name
CREATE OR REPLACE TASK "SNOWFLAKE_LEARNING_DB"."PUBLIC"."TRANSFORM_BETA_CONNECT_DATA_TASK"
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '15 MINUTE' -- Or your desired frequency
AS
    CREATE OR REPLACE TABLE "SNOWFLAKE_LEARNING_DB"."PUBLIC"."TRANSFORMED_BETA_CONNECT_DATA" AS
    SELECT
        id,
        first_name,
        last_name,
        email,
        gender,
        ip_address,
        client_id,
        phone_number,
        country_code,
        data_usage,
        CASE
            WHEN "STANDARDIZED_BILLING_CYCLE" = 'monthly' THEN 'Monthly'
            WHEN "STANDARDIZED_BILLING_CYCLE" = 'yearly' THEN 'Yearly'
            WHEN "STANDARDIZED_BILLING_CYCLE" = 'quarterly' THEN 'Quarterly'
            ELSE 'Other'
        END AS standardized_billing_cycle,
        internet_speed,
        contract_start_date,
        contract_end_date,
        monthly_bill_amount,
        service_type,
        contract_duration_days
    FROM "SNOWFLAKE_LEARNING_DB"."PUBLIC"."BETA_CONNECT_DATA";

-- Resume the task using its fully qualified name
ALTER TASK "SNOWFLAKE_LEARNING_DB"."PUBLIC"."TRANSFORM_BETA_CONNECT_DATA_TASK" RESUME;
