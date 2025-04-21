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

def calculate_tvl(amount_token0, amount_token1, sqrt_price, price_token0=None, price_token1=None):
    # 如果已知两个 token 的价格，直接计算 TVL
    if price_token0 is not None and price_token1 is not None:
        return amount_token0 * price_token0 + amount_token1 * price_token1

    # 如果只知道一个 token 的价格 + sqrt_price，则推算另一个
    price_ratio = (sqrt_price / (2 ** 96)) ** 2

    if price_token0 is not None:
        price_token1 = price_token0 * price_ratio
    elif price_token1 is not None:
        price_token0 = price_token1 / price_ratio
    else:
        raise ValueError("At least one token price must be known.")

    tvl = amount_token0 * price_token0 + amount_token1 * price_token1
    return tvl
