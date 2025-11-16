CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------------------
    -- 1. Load CRM Customer Info
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        TRY_CONVERT(DATETIME2, TRY_CONVERT(VARCHAR(30), cst_create_date))   
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(30), cst_create_date)) DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;


    ---------------------------------------------------------------------
    -- 2. Load CRM Product Info
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
    SUBSTRING(prd_key, 7, LEN(prd_key)),
    prd_nm,
    ISNULL(prd_cost, 0),

    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    -- Direct conversion (YYYY-MM-DD works)
    TRY_CONVERT(DATE, prd_start_dt) AS prd_start_dt,

    -- End date = 1 day before next prd_start_dt
    TRY_CONVERT(
        DATE,
        DATEADD(
            DAY, -1,
            LEAD(TRY_CONVERT(DATE, prd_start_dt))
            OVER (
                PARTITION BY prd_key
                ORDER BY TRY_CONVERT(DATE, prd_start_dt)
            )
        )
    ) AS prd_end_dt
FROM bronze.crm_prd_info;


    ---------------------------------------------------------------------
    -- 3. Load CRM Sales Details
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(8), sls_order_dt)),
        TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(8), sls_ship_dt)),
        TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(8), sls_due_dt)),

        CASE 
            WHEN sls_sales IS NULL 
                OR sls_sales <= 0 
                OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;


    ---------------------------------------------------------------------
    -- 4. Load ERP Customer AZ12
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END,
        TRY_CONVERT(DATE, TRIM(REPLACE(bdate, '"', ''))),
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;


    ---------------------------------------------------------------------
    -- 5. Load ERP Location A101
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;


    ---------------------------------------------------------------------
    -- 6. Load ERP PX Category G1V2
    ---------------------------------------------------------------------
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

END;
GO
