# -*- coding: utf-8 -*-
import pymysql
import requests
import time
import sys
from datetime import datetime

# ==========================================
# 1. 数据库配置
# ==========================================
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'userisahsj',
    'database': 'cc',
    'charset': 'utf8mb4'
}

# ==========================================
# 2. 工具函数
# ==========================================
def log(msg):
    """打印带时间的标准日志"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def get_ip_info(ip):
    """
    仅使用太平洋电脑网验证，并增加详细日志输出
    """
    try:
        url = f"https://whois.pconline.com.cn/ipJson.jsp?ip={ip}&json=true"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        res = requests.get(url, headers=headers, timeout=8)
        
        if res.status_code == 200 and res.text.strip():
            data = res.json()
            addr = data.get('addr', '')
            city = data.get('city', '')
            
            # 只要城市或地址描述中包含“洛阳”即认定为本地 IP
            is_ly = '洛阳' in city or '洛阳' in addr
            return is_ly, f"城市:{city} | 地址:{addr}"
            
        return False, f"接口异常(HTTP {res.status_code})"
    except Exception as e:
        return False, f"查询报错: {str(e)}"

# ==========================================
# 3. 主程序逻辑
# ==========================================
def main():
    log("🚀 开始执行 [洛阳本地IP防误杀] 巡检 (修正版)...")

    conn = None
    try:
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset=DB_CONFIG['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        
        # 只查询没有备注或备注非“已校验”的 DROP 数据
        query_sql = "SELECT id, addr FROM firewall WHERE action = 'DROP' AND (remark NOT LIKE '%已校验%' OR remark IS NULL)"
        cursor.execute(query_sql)
        dropped_ips = cursor.fetchall()

        if not dropped_ips:
            log("✅ 当前没有需要校验的拉黑 IP。")
            return

        total = len(dropped_ips)
        log(f"🔍 检查到 {total} 个待处理 IP...")

        unlocked_count = 0
        for index, item in enumerate(dropped_ips, 1):
            ip_addr = item['addr']
            ip_id = item['id']
            
            is_ly, info = get_ip_info(ip_addr)
            
            # 实时打印详情，方便你核对为什么会被判定
            sys.stdout.write(f"\r进度: [{index}/{total}] 校验: {ip_addr.ljust(15)} -> {info} ")
            sys.stdout.flush()

            if is_ly:
                log(f"\n🚨 确认洛阳 IP！正在解锁: {ip_addr}")
                update_sql = "UPDATE firewall SET action = 'ACCEPT', remark = %s WHERE id = %s"
                remark_msg = f"AI解锁: 洛阳({datetime.now().strftime('%m%d')})"
                cursor.execute(update_sql, (remark_msg, ip_id))
                unlocked_count += 1
            else:
                # 记录非洛阳的校验结果
                mark_sql = "UPDATE firewall SET remark = %s WHERE id = %s"
                cursor.execute(mark_sql, (f"非洛阳IP-已校验({datetime.now().strftime('%m%d')})", ip_id))

            conn.commit()
            time.sleep(0.6) 

        log(f"\n✨ 巡检完成。本次共解锁洛阳 IP {unlocked_count} 个。")

    except Exception as e:
        log(f"\n❌ 出错: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()