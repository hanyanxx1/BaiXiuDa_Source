# 用于解决：
# 1. 本地 IP 被误杀（如：127.0.0.1）
# 2. 友军 IP 被误杀（如：114.114.114.114）

# -*- coding: utf-8 -*-
import pymysql
import requests
import time
from datetime import datetime

# 打印带时间的标准日志
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# 调用免费公开的 IP 归属地接口（太平洋网络，国内速度最快且不限制频次）
def is_luoyang_ip(ip):
    try:
        url = f"https://whois.pconline.com.cn/ipJson.jsp?ip={ip}&json=true"
        # 伪装成浏览器访问，防止被拦截
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers, timeout=5)
        data = res.json()
        
        # 只要城市或者详细地址里包含“洛阳”，就认定为友军
        if '洛阳' in data.get('city', '') or '洛阳' in data.get('addr', ''):
            return True
    except Exception as e:
        log(f"查询IP {ip} 归属地超时或失败: {e}")
    return False

def main():
    log("🚀 开始执行 [洛阳本地IP防误杀] 每日晨间巡检...")
    
    # 连接 cc 数据库
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='userisahsj',
            database='cc',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        
        # 1. 查出所有当前被拉黑 (DROP) 的 IP
        cursor.execute("SELECT id, addr FROM firewall WHERE action = 'DROP'")
        dropped_ips = cursor.fetchall()
        
        if not dropped_ips:
            log("✅ 当前没有任何被拉黑的 IP，巡检结束。")
            return
            
        log(f"🔎 共发现 {len(dropped_ips)} 个被拉黑的 IP，开始逐一排查归属地...")
        
        recover_count = 0
        for row in dropped_ips:
            ip_id = row['id']
            ip_addr = row['addr']
            
            # 如果判定为洛阳 IP
            if is_luoyang_ip(ip_addr):
                log(f"⚠️ 发现【洛阳友军】IP被误杀: {ip_addr}，正在执行特赦恢复...")
                # 将状态改回 ACCEPT，并且把优先级改回 1
                update_sql = "UPDATE firewall SET action = 'ACCEPT', priority = 1 WHERE id = %s"
                cursor.execute(update_sql, (ip_id,))
                recover_count += 1
            
            # 休眠 0.3 秒，防止接口请求过快被临时封禁
            time.sleep(0.3)
            
        # 提交所有修改
        conn.commit()
        log(f"🎉 巡检完成！本次共成功解封了 {recover_count} 个洛阳本地 IP。")
        
    except Exception as e:
        log(f"❌ 数据库执行致命错误: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == '__main__':
    main()