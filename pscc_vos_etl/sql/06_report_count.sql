SELECT COUNT(*) FROM etl_merged_cdr WHERE DATE( cc_start_time ) = '2026-03-05'

SELECT COUNT(*) FROM etl_merged_cdr WHERE DATE( cc_start_time ) = '2026-03-05' AND cc_admin_id = 1