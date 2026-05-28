import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
import urllib.parse

# ==========================================
# 1. 数据库配置 (FinalShell 隧道直连版)
# ==========================================
CC_DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'userisahsj',
    'database': 'cc',
    'charset': 'utf8'
}

REPORT_DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'userisahsj',
    'database': 'pscc_report',
    'charset': 'utf8'
}

VOS_DB_CONFIG = {
    'host': '192.168.11.112',
    'port': 3306,
    'user': 'root',
    'password': 'Beijing@china',
    'database': 'vos3000',
    'charset': 'utf8'
}

# ==========================================
# 2. 单日清洗核心函数
# ==========================================
def process_single_date(target_date):
    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    table_suffix = target_date_obj.strftime("%Y%m%d") 
    print(f"\n[{target_date}] 🚀 开始执行该日话单对账...")

    try:
        cc_pwd = urllib.parse.quote_plus(CC_DB_CONFIG['password'])
        vos_pwd = urllib.parse.quote_plus(VOS_DB_CONFIG['password'])
        report_pwd = urllib.parse.quote_plus(REPORT_DB_CONFIG['password'])

        cc_engine_url = f"mysql+pymysql://{CC_DB_CONFIG['user']}:{cc_pwd}@{CC_DB_CONFIG['host']}:{CC_DB_CONFIG['port']}/{CC_DB_CONFIG['database']}?charset={CC_DB_CONFIG['charset']}"
        cc_engine = create_engine(cc_engine_url)

        vos_engine_url = f"mysql+pymysql://{VOS_DB_CONFIG['user']}:{vos_pwd}@{VOS_DB_CONFIG['host']}:{VOS_DB_CONFIG['port']}/{VOS_DB_CONFIG['database']}?charset={VOS_DB_CONFIG['charset']}"
        vos_engine = create_engine(vos_engine_url)
        
        report_engine_url = f"mysql+pymysql://{REPORT_DB_CONFIG['user']}:{report_pwd}@{REPORT_DB_CONFIG['host']}:{REPORT_DB_CONFIG['port']}/{REPORT_DB_CONFIG['database']}?charset={REPORT_DB_CONFIG['charset']}"
        report_engine = create_engine(report_engine_url)

        admin_sql = "SELECT id FROM admin"
        admin_df = pd.read_sql(admin_sql, cc_engine)
        admin_ids = admin_df['id'].tolist()
        
        for current_admin_id in admin_ids:
            print(f"  👉 正在处理租户 Admin_ID: {current_admin_id} ...")
            
            # 1. 获取网关
            gateway_sql = f"SELECT name FROM gateway WHERE admin_id = {current_admin_id}"
            gateway_df = pd.read_sql(gateway_sql, cc_engine)
            if gateway_df.empty:
                print(f"     ⚠️ 租户 {current_admin_id} 未配置网关，跳过。")
                continue
            caller_list_str = ", ".join([f"'{name}'" for name in gateway_df['name'].tolist()])
            
            # 2. 提取 CC
            try:
                with open('../sql/02_extract_cc.sql', 'r', encoding='utf-8') as f:
                    cc_sql_template = f.read()
                cc_sql = cc_sql_template.format(table_suffix=table_suffix, start_date=target_date, current_admin_id=current_admin_id)
                df_cc = pd.read_sql(cc_sql, cc_engine)
                print(f"     ✅ [对账点1] CC侧提取有效话单: {len(df_cc)} 条")
            except ProgrammingError as e:
                if "doesn't exist" in str(e):
                    print(f"     ⚠️ CC库不存在表 {table_suffix}，跳过。")
                    break 
                raise e

            if df_cc.empty:
                print(f"     ✅ 租户 {current_admin_id} 今日无有效话单，跳过。")
                continue

            # 3. 提取 VOS
            try:
                with open('../sql/03_extract_vos.sql', 'r', encoding='utf-8') as f:
                    vos_sql_template = f.read()
                vos_sql = vos_sql_template.format(table_suffix=table_suffix, caller_id_list=caller_list_str)
                df_vos = pd.read_sql(vos_sql, vos_engine)
                print(f"     ✅ [对账点2] VOS侧提取底层信令: {len(df_vos)} 条")
            except ProgrammingError as e:
                if "doesn't exist" in str(e):
                     print(f"     ⚠️ VOS库不存在表 {table_suffix}，跳过。")
                     break
                raise e

# ==========================================
# [修改开始：根据真实业务截图，放宽时间差至60秒，收紧时长差至10秒，并强制主被叫双重验证]
# ==========================================
            print(f"     🔄 [对账点3] 正在执行黄金四法则融合 (主被叫一致, 时间差<=60s, 时长差<=10s)...")

            # 保留原有的 target_number 映射，供最终保留列使用
            df_cc['target_number'] = df_cc['cc_telephone']

            # 1. 提取基础比对列 (条件1和条件2：主叫和被叫必须严格相等)
            df_cc['match_caller'] = df_cc['cc_gateway_name'].astype(str)
            df_cc['match_callee'] = df_cc['cc_telephone'].astype(str)
            
            df_vos['match_caller'] = df_vos['vos_caller'].astype(str)
            df_vos['match_callee'] = df_vos['vos_callee'].astype(str)

            # 2. 时间列准备
            df_cc['match_time'] = pd.to_datetime(df_cc['cc_start_time'])
            df_vos['match_time'] = pd.to_datetime(df_vos['vos_start_time'])

            # 必须在合并前按时间排序
            df_cc = df_cc.sort_values('match_time')
            df_vos = df_vos.sort_values('match_time')

            # 3. 容差匹配 (满足条件1、2、3：主叫=主叫，被叫=被叫，开始时间差 <= 60秒)
            merged_df = pd.merge_asof(
                df_cc, df_vos, 
                on='match_time', 
                by=['match_caller', 'match_callee'], 
                tolerance=pd.Timedelta('60s'),  # <--- 核心修改：为了囊括截图中的19秒误差，放宽至60秒
                direction='nearest'
            )

            # 4. 强制校验条件4 (通话时长误差 <= 10秒)
            # 计算时长误差绝对值
            merged_df['duration_diff'] = (merged_df['cc_billsec'] - merged_df['vos_hold_time']).abs()
            
            # 揪出那些虽然时间对上了，但时长误差超过 10 秒的数据
            invalid_duration_mask = merged_df['duration_diff'] > 10
            
            # 👇 修改点 1：把 'vos_agentfee' 加入清理名单，如果没匹配上或者误差大，代理费直接作废
            vos_columns_to_clear = ['vos_caller', 'vos_start_time', 'vos_hold_time', 'vos_callee_gateway', 'vos_agentfee', 'vos_agentaccount']
            merged_df.loc[invalid_duration_mask, vos_columns_to_clear] = None
# ==========================================
# [修改结束]
# ==========================================

            # 5. 保留列
            columns_to_keep = [
                'cc_admin_id', 'target_number', 
                'cc_start_time', 'cc_billsec', 'cc_consume', 'cc_status', 'cc_gateway_name',
                'vos_caller', 'vos_start_time', 'vos_hold_time', 'vos_callee_gateway', 'vos_agentfee', 'vos_agentaccount'
            ]
            merged_df.rename(columns={'vos_caller': 'vos_caller_number'}, inplace=True)
            columns_to_keep[7] = 'vos_caller_number'
            final_df = merged_df[columns_to_keep].copy()

            final_df['vos_agentfee'] = final_df['vos_agentfee'].fillna(0.00)
            final_df['vos_agentaccount'] = final_df['vos_agentaccount'].fillna('')

            final_df['vos_agentfee'] = final_df['vos_agentfee'].fillna(0.00)
            # 6. 先删旧数据（防重复写入）
            delete_sql = f"DELETE FROM etl_merged_cdr WHERE cc_admin_id = {current_admin_id} AND DATE(cc_start_time) = '{target_date}'"
            with report_engine.begin() as conn:
                conn.execute(text(delete_sql))
            print(f"     🧹 [清理历史] 已清理该租户 {target_date} 可能存在的旧数据 (幂等保护)。")

            # 7. 追加写入报表库
            final_df.to_sql(name='etl_merged_cdr', con=report_engine, if_exists='append', index=False)
            print(f"     🎉 [最终核对] 租户 {current_admin_id} 成功入库融合数据: {len(final_df)} 条 (应等于对账点1的数量)。\n")

    except Exception as e:
        print(f"[{target_date}] ❌ 处理失败，错误信息: {str(e)}")

# ==========================================
# 3. 程序入口与参数解析
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="PSCC & VOS3000 跨库话单清洗同步脚本")
    parser.add_argument('--date', type=str, help='同步指定单天数据, 格式: YYYY-MM-DD')
    parser.add_argument('--start', type=str, help='批量同步开始日期, 格式: YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='批量同步结束日期, 格式: YYYY-MM-DD')
    args = parser.parse_args()

    date_list = []
    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append((start_date + timedelta(days=i)).strftime("%Y-%m-%d"))
    elif args.date:
        date_list.append(args.date)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_list.append(yesterday)

    print(f"系统即将执行以下日期的对账任务: {date_list}")

    for target_date in date_list:
        process_single_date(target_date)

if __name__ == "__main__":
    main()

"""
=============================================================================
【PSCC & VOS3000 跨库话单清洗同步脚本 - 测试与调用用例】

本脚本基于命令行参数运行，支持单日跑批、历史数据区间重跑以及默认自动跑批。
请在终端 (Terminal/PowerShell) 中进入 scripts 目录执行以下命令：

[用例 1] 默认自动跑批 (常用于定时任务 crontab)
描述：不带任何参数执行，系统会自动计算并处理“昨天 (T-1)”的全量对账数据。
命令：
    python main_sync.py

[用例 2] 指定单日精准跑批 (常用于排查某一天的问题或补跑数据)
描述：使用 --date 参数，强制系统处理指定日期的对账。
格式：YYYY-MM-DD
命令：
    python main_sync.py --date 2026-03-05

[用例 3] 历史区间批量重跑 (常用于规则变更后的大范围数据洗牌)
描述：同时使用 --start 和 --end 参数，系统会自动生成该区间内所有的日期列表并按序依次执行。
格式：YYYY-MM-DD
命令：
    python main_sync.py --start 2026-03-01 --end 2026-03-05
    (注: 该命令会依次串行执行 3月1日、3月2日...直到 3月5日 的对账任务)

[用例 4] 查看帮助文档
描述：在命令行中打印当前脚本支持的所有参数说明。
命令：
    python main_sync.py --help
    (或 python main_sync.py -h)
=============================================================================
"""