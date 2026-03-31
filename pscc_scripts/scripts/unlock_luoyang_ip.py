# -*- coding: utf-8 -*-
import pymysql
import requests
import time
import sys
from datetime import datetime

# ==========================================
# 1. 数据库配置 (服务器本地连接版)
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
    """打印带时间的标准日志，增加 flush 确保实时显示进度"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def get_ip_info(ip):
    """
    获取 IP 归属地详细信息 (仅使用太平洋电脑网验证)
    """
    try:
        # 太平洋电脑网接口
        url = f"https://whois.pconline.com.cn/ipJson.jsp?ip={ip}&json=true"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        res = requests.get(url, headers=headers, timeout=8)
        
        if res.status_code == 200 and res.text.strip():
            data = res.json()
            addr = data.get('addr', '未知地址')
            city = data.get('city', '')
            
            # 判断逻辑：城市名或详细地址包含“洛阳”
            is_ly = '洛阳' in city or '洛阳' in addr
            return is_ly, addr
            
        return False, f"接口返回异常(HTTP {res.status_code})"
    except Exception as e:
        return False, f"查询出错: {str(e)}"

# ==========================================
# 3. 主程序逻辑
# ==========================================
def main():
    log("🚀 开始执行 [洛阳本地IP防误杀] 巡检...")

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
        
        # 查询没有标记过“已校验”的拉黑 IP
        query_sql = "SELECT id, addr FROM firewall WHERE action = 'DROP' AND (remark NOT LIKE '%已校验%' OR remark IS NULL)"
        cursor.execute(query_sql)
        dropped_ips = cursor.fetchall()

        if not dropped_ips:
            log("✅ 当前没有新增的拉黑 IP 需要校验，巡检结束。")
            return

        total = len(dropped_ips)
        log(f"🔍 检查到 {total} 个待处理 IP，开始通过太平洋电脑网校验...")

        unlocked_count = 0
        for index, item in enumerate(dropped_ips, 1):
            ip_addr = item['addr']
            ip_id = item['id']
            
            # 获取归属地和判断结果
            is_ly, location = get_ip_info(ip_addr)
            
            # 【输出优化】实时显示当前 IP 对应的地址
            sys.stdout.write(f"\r进度: [{index}/{total}] 正在校验: {ip_addr.ljust(15)} -> 归属地: {location} ... ")
            sys.stdout.flush()

            if is_ly:
                log(f"\n🚨 发现误杀！IP {ip_addr} ({location}) 为洛阳 IP，正在解锁...")
                update_sql = "UPDATE firewall SET action = 'ACCEPT', remark = %s WHERE id = %s"
                remark_msg = f"AI自动解锁: 洛阳本地IP ({datetime.now().strftime('%m%d')})"
                cursor.execute(update_sql, (remark_msg, ip_id))
                unlocked_count += 1
            else:
                # 记录已校验，防止下次重复请求，备注中包含具体归属地
                mark_sql = "UPDATE firewall SET remark = %s WHERE id = %s"
                cursor.execute(mark_sql, (f"非洛阳IP-{location}-已校验({datetime.now().strftime('%m%d')})", ip_id))

            # 提交单条更新
            conn.commit()
            
            # 控制频率，避免接口封禁频率过快
            time.sleep(0.6) 

        log(f"\n✨ 巡检完成。本次共解锁洛阳 IP {unlocked_count} 个。")

    except Exception as e:
        log(f"\n❌ 运行出错: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()