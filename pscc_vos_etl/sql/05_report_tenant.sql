-- ====================================================
-- 报表查询：独立租户汇总 (包含合计行)
-- 适用场景：各个租户登录自己后台时，只能看到自己账号下的数据
-- 参数说明：
--   {start_date} : 开始日期 (例如 '2026-03-01')
--   {end_date}   : 结束日期 (例如 '2026-03-05')
--   {admin_id}   : 租户ID (例如 1)
-- ====================================================

SELECT 
    `落地名称`, 
    `成功`, 
    `话费(元)`, 
    `每单话费(元)`, 
    `时长(秒)`, 
    `平均时长(秒)`
FROM (
    -- 上半部分：各网关明细数据 (带租户隔离)
    SELECT 
        vos_callee_gateway AS '落地名称',
        CONCAT(SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END), '(', ROUND(SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2), '%)') AS '成功',
        ROUND(SUM(cc_consume), 2) AS '话费(元)',
        CAST(ROUND(SUM(cc_consume) / NULLIF(SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END), 0), 2) AS CHAR) AS '每单话费(元)',
        SUM(vos_hold_time) AS '时长(秒)',
        CAST(ROUND(SUM(vos_hold_time) / COUNT(*), 2) AS CHAR) AS '平均时长(秒)',
        0 AS sort_order, 
        SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END) AS success_count
    FROM 
        etl_merged_cdr 
    WHERE 
        cc_start_time >= '{start_date} 00:00:00' 
        AND cc_start_time <= '{end_date} 23:59:59'
        AND cc_admin_id = {admin_id}  -- 👉 核心：租户数据隔离屏障
        AND vos_callee_gateway IS NOT NULL
    GROUP BY 
        vos_callee_gateway

    UNION ALL

    -- 下半部分：该租户全局合计数据
    SELECT 
        '合计' AS '落地名称',
        CAST(SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END) AS CHAR) AS '成功',
        ROUND(SUM(cc_consume), 2) AS '话费(元)',
        CONCAT('平均: ', ROUND(SUM(cc_consume) / NULLIF(SUM(CASE WHEN cc_status = 4 THEN 1 ELSE 0 END), 0), 2)) AS '每单话费(元)',
        SUM(vos_hold_time) AS '时长(秒)',
        CONCAT('平均: ', ROUND(SUM(vos_hold_time) / COUNT(*), 2)) AS '平均时长(秒)',
        1 AS sort_order,  
        0 AS success_count
    FROM 
        etl_merged_cdr 
    WHERE 
        cc_start_time >= '{start_date} 00:00:00' 
        AND cc_start_time <= '{end_date} 23:59:59'
        AND cc_admin_id = {admin_id}  -- 👉 核心：租户数据隔离屏障
        AND vos_callee_gateway IS NOT NULL
) AS final_report

ORDER BY 
    sort_order ASC, 
    success_count DESC;