from decimal import Decimal, ROUND_HALF_UP

def prepare_apy_for_sql(apr, n, max_apy=Decimal('99999999.99')) -> float:
    """
    计算并格式化 APY，确保符合 SQL 中 DECIMAL(10,2) NOT NULL 的要求。
    
    参数：
        apr (float|Decimal): 年利率，如 0.05 表示 5%
        n (int): 一年复利次数，如 12、365
        max_apy (Decimal): 数据库允许的最大值，默认99999999.99

    返回：
        float: 可直接写入数据库的 APY 百分比值（如 5.25 表示5.25%）
    """
    if apr is None or n is None or n == 0:
        raise ValueError("APR 和 n 都不能为空，且 n 不能为 0")

    apr = Decimal(str(apr))
    n = Decimal(str(n))

    apy = (1 + apr / n) ** n - 1
    apy_percent = apy * 100

    if apy_percent > max_apy:
        print(f"⚠️ APY 超出最大范围，截断为 {max_apy}")
        apy_percent = max_apy

    apy_percent = apy_percent.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    return float(apy_percent)
