import mysql.connector
from decimal import Decimal
from datetime import date
from utils.utils import last_quarter, last_year

def calculate_yoy(current, previous):
    """计算同比增长率 (%)，若数据不合法则返回 None"""
    try:
        if current is None or previous is None or previous == 0:
            return None
        return float((Decimal(current) - Decimal(previous)) / Decimal(previous) * 100)
    except:
        return None


def execute_sql(query, params=None, fetch=False):
    """简化的数据库查询方法"""
    cfg = {
        'host': '194.233.75.155',
        'port': 3306,
        'user': 'jtyd_db_front_user',
        'password': 'dfsw33DG3!',
        'database': 'queryweb3_front'
    }

    conn = mysql.connector.connect(**cfg)
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        result = None
        if fetch:
            result = cursor.fetchall()
        elif cursor.with_rows:
            cursor.fetchall()  # 防止 unread result 错误
        conn.commit()
        return result
    finally:
        cursor.close()
        conn.close()


def test_yoy(token_id, prev_year_date, volume_usd_current, tx_count):
    # 获取去年数据
    res = execute_sql("""
        SELECT volume, txns_count FROM fact_token_daily_stats 
        WHERE token_id = %s AND date = %s
    """, (token_id, prev_year_date), fetch=True)


    volume_year = None
    txns_count_year = None
    if res and res[0][0] is not None:
        volume_year = res[0][0]
        txns_count_year = res[0][1]
    
    print("volume_year", volume_year)
    print("txns_count_year", txns_count_year)
    
    volume_yoy = calculate_yoy(volume_usd_current, volume_year)
    txns_yoy = calculate_yoy(tx_count, txns_count_year)

    print(f"Token ID: {token_id}")
    print(f"Current volume: {volume_usd_current}, Last year volume: {volume_year}, YoY: {volume_yoy}%")
    print(f"Current txns: {tx_count}, Last year txns: {txns_count_year}, YoY: {txns_yoy}%")


# 🧪 示例调用
if __name__ == "__main__":
    token_id = 195385

    # 假设今天是当前统计日
    current_date = date(2025, 5, 2)

    # 计算去年同日和上一季度同日
    prev_year = last_year(current_date)
    prev_quarter = last_quarter(current_date)

    print("当前日期:", current_date)
    print("去年同期:", prev_year)
    print("上季度:", prev_quarter)
    
    # current_date = '2025-04-28'
    # prev_year_date = '2024-04-28'
    volume_usd_current = Decimal('1234567.89')  # 当前交易量
    tx_count = 23456  # 当前交易数

    test_yoy(token_id, prev_year, volume_usd_current, tx_count)
    test_yoy(token_id, prev_quarter, volume_usd_current, tx_count)
