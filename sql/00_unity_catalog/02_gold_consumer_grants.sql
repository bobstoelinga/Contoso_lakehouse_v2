-- =============================================================================
-- 02_gold_consumer_grants.sql
-- Objectniveau voor Gold-consumenten. Dit script draait na de Gold-DDL, zodat
-- schemarechten geen leesrecht op nieuwe of interne objecten impliceren.
-- =============================================================================

-- Historische datamart: expliciete tabellen.
GRANT SELECT ON TABLE contoso_gold_${env}.historical.dim_customer_hist TO `${bi_analysts_group}`;
GRANT SELECT ON TABLE contoso_gold_${env}.historical.dim_product_hist TO `${bi_analysts_group}`;
GRANT SELECT ON TABLE contoso_gold_${env}.historical.fct_sales_hist TO `${bi_analysts_group}`;
GRANT SELECT ON TABLE contoso_gold_${env}.historical.dim_employee_hist TO `${bi_analysts_group}`;
GRANT SELECT ON TABLE contoso_gold_${env}.historical.fct_returns_hist TO `${bi_analysts_group}`;

-- Actuele datamart: uitsluitend publieke views, nooit de fysieke slots.
GRANT SELECT ON VIEW contoso_gold_${env}.current.dim_customer TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.dim_product TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.fct_sales TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.dim_employee TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.dim_date TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.fct_returns TO `${bi_analysts_group}`;
GRANT SELECT ON VIEW contoso_gold_${env}.current.v_gold_freshness TO `${bi_analysts_group}`;
