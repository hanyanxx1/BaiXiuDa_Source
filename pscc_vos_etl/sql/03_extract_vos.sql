SELECT 
  callere164 AS 'vos_caller',
  calleee164 AS 'vos_callee',
  FROM_UNIXTIME( starttime / 1000 ) AS 'vos_start_time',
  holdtime AS 'vos_hold_time',
  calleegatewayid AS 'vos_callee_gateway'
FROM 
  e_cdr_{table_suffix} 
WHERE 
  holdtime >= 1 
  -- 核心升级：不写死！让 Python 脚本自动把 CC 库里合法的网关号码填到这里面
  AND callere164 IN ({caller_id_list});