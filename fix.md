To see corrected numbers

Re-run in this order:

01_observe/01_feat_cluster_utilization      # rebuild with proper node_count\
01_observe/02_feat_job_cost_trend           # rebuild with fixed list_prices + jobs joins\
01_observe/03_feat_job_health               # already fixed last turn\
07_report/01_cost_command_center            # republishes views with windowed price join\
07_report/02_cluster_health_map             # republishes with node_count + latest-cluster filter\
07_report/03_job_reliability                # already fixed last turn

How to verify against actual runs

I can't query your workspace from here, but two cross-checks that'll prove the fixes worked:

1. Cost reconciliation — for any single day, run:\
   SELECT SUM(u.usage_quantity \* p.pricing.default) AS daily_usd\
   FROM system.billing.usage u\
   JOIN system.billing.list_prices p\
   ON [u.cloud](http://u.cloud) = [p.cloud](http://p.cloud) AND u.sku_name = p.sku_name\
   AND u.usage_start_time &gt;= p.price_start_time\
   AND (p.price_end_time IS NULL OR u.usage_start_time &lt; p.price_end_time)\
   WHERE u.usage_date = DATE''
2. Compare with what Databricks' built-in Account Usage page reports for the same day. They should match within rounding. Before the fix, the\
   IntelliOps number would be \~N× higher where N = average number of historical prices per SKU.
3. Job-runs reconciliation — for EBC agent — sales_pulse, run:\
   SELECT COUNT(DISTINCT run_id) FROM system.lakeflow.job_run_timeline\
   WHERE job_id = \
   AND period_start_time &gt;= CURRENT_DATE - INTERVAL 30 DAYS;
4. That distinct count should match feat_job\_[health.total](http://health.total)\_runs after the rebuild. And both should be roughly consistent with what the Jobs UI shows (it\
   doesn't expose a total count directly, but daily run counts × 30 should be in the same ballpark).