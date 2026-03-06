import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# ==========================================
# 1. 数据库配置 (FinalShell 隧道直连版)
# ==========================================
CC_DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 33066,
    'user': 'root',
    'password': 'userisahsj',
    'database': 'cc',
    'charset': 'utf8'
}

REPORT_DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 33066,
    'user': 'root',
    'password': 'userisahsj',
    'database': 'pscc_report',
    'charset': 'utf8'
}

VOS_DB_CONFIG = {
    'host': '8.130.15.135',
    'port': 3306,
    'user': 'hanyan',
    'password': 'BAIXIUDA@@han138388',
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
        cc_engine_url = f"mysql+pymysql://{CC_DB_CONFIG['user']}:{CC_DB_CONFIG['password']}@{CC_DB_CONFIG['host']}:{CC_DB_CONFIG['port']}/{CC_DB_CONFIG['database']}?charset={CC_DB_CONFIG['charset']}"
        cc_engine = create_engine(cc_engine_url)

        vos_engine_url = f"mysql+pymysql://{VOS_DB_CONFIG['user']}:{VOS_DB_CONFIG['password']}@{VOS_DB_CONFIG['host']}:{VOS_DB_CONFIG['port']}/{VOS_DB_CONFIG['database']}?charset={VOS_DB_CONFIG['charset']}"
        vos_engine = create_engine(vos_engine_url)
        
        report_engine_url = f"mysql+pymysql://{REPORT_DB_CONFIG['user']}:{REPORT_DB_CONFIG['password']}@{REPORT_DB_CONFIG['host']}:{REPORT_DB_CONFIG['port']}/{REPORT_DB_CONFIG['database']}?charset={REPORT_DB_CONFIG['charset']}"
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
            
            if df_vos.empty:
                 print(f"     ⚠️ VOS库中未找到该租户对应的底层话单，跳过。")
                 continue

# ==========================================
# [修改开始：将容差从 3s 放宽到 60s，给足客户响铃接听的时间差]
# ==========================================
            print(f"     🔄 [对账点3] 正在进行 Pandas 60秒 容差融合...")

            # 4. Pandas 容差匹配
            df_cc['target_number'] = df_cc['cc_telephone']
            df_vos['target_number'] = df_vos['vos_callee']
            df_cc['match_time'] = pd.to_datetime(df_cc['cc_start_time'])
            df_vos['match_time'] = pd.to_datetime(df_vos['vos_start_time'])

            df_cc = df_cc.sort_values('match_time')
            df_vos = df_vos.sort_values('match_time')

            merged_df = pd.merge_asof(
                df_cc, df_vos, on='match_time', by='target_number',
                tolerance=pd.Timedelta('60s'), direction='nearest'  # <--- 核心修改：60秒容差
            )
# ==========================================
# [修改结束]
# ==========================================

            # 5. 保留列
            columns_to_keep = [
                'cc_admin_id', 'target_number', 
                'cc_start_time', 'cc_duration', 'cc_consume', 'cc_status', 'cc_gateway_name',
                'vos_caller', 'vos_start_time', 'vos_hold_time', 'vos_callee_gateway'
            ]
            merged_df.rename(columns={'vos_caller': 'vos_caller_number'}, inplace=True)
            columns_to_keep[7] = 'vos_caller_number'
            final_df = merged_df[columns_to_keep].copy()

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