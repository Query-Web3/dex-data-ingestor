from decimal import Decimal, ROUND_HALF_UP,getcontext
from dateutil.relativedelta import relativedelta
from decimal import Decimal, getcontext, ROUND_DOWN, InvalidOperation
from typing import Optional, Tuple
import logging


# 设置精度
getcontext().prec = 50

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
        logging.warning(f"⚠️ APY 超出最大范围，截断为 {max_apy}")
        apy_percent = max_apy

    apy_percent = apy_percent.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    return float(apy_percent)


def calculate_tvl(amount_token0, amount_token1, sqrt_price, token0_decimals, token1_decimals):
    # 将整数数量转换为实际数量（根据 decimals）
    real_amount_token0 = Decimal(amount_token0) / (Decimal(10) ** token0_decimals)
    real_amount_token1 = Decimal(amount_token1) / (Decimal(10) ** token1_decimals)

    # 估算 price
    price = Decimal(sqrt_price)

    # 默认 token0 价格为 1 USD，计算 token1 价格
    token0_price = Decimal(1)
    token1_price = price * token0_price

    tvl = real_amount_token0 * token0_price + real_amount_token1 * token1_price
    return float(tvl)


# def calculate_tvl(amount_token0, amount_token1, sqrt_price, token0_decimals, token1_decimals):
#     amount_token0 = Decimal(str(amount_token0)) / (Decimal(10) ** token0_decimals)
#     amount_token1 = Decimal(str(amount_token1)) / (Decimal(10) ** token1_decimals)
#     sqrt_price = Decimal(str(sqrt_price))

#     P = sqrt_price ** 2
#     estimated_token0_price = Decimal("1.0")
#     estimated_token1_price = P * estimated_token0_price

#     tvl = amount_token0 * estimated_token0_price + amount_token1 * estimated_token1_price
#     return float(tvl)

def calculate_tvl_usd(amount_token0, amount_token1, sqrt_price, price_token0=None, price_token1=None):
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

def last_quarter(date):
    print("quarter date：", date)
    if not date:
        return None
    # return date - relativedelta(months=3)    
    return date - relativedelta(days=14)   

def last_year(date):
    print("year date：", date)
    if not date:
        return None
    # return date - relativedelta(years=1)
    return date - relativedelta(days=14)

def calculate_yoy(current, previous_year):
    """
    计算同比增长率（YoY）
    - 如果参数无效则返回 None
    """
    print("current", current)
    print("previous_year", previous_year)
    if current is None or previous_year is None or previous_year == 0:
        return None
    return round((float(current) - float(previous_year)) / float(previous_year) * 100, 2)

def calculate_qoq(current, previous_quarter):
    """
    计算环比增长率（QoQ）
    - 如果参数无效则返回 None
    """
    print("current", current)
    print("previous_quarter", previous_quarter)
    if current is None or previous_quarter is None or previous_quarter == 0:
        return None
    return round((float(current) - float(previous_quarter)) / float(previous_quarter) * 100, 2)



# 设置高精度以支持 decimal(36,18)
getcontext().prec = 50
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Optional, Tuple

def calculate_token_prices(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_usd: Optional[Decimal] = None,
    token1_usd: Optional[Decimal] = None
) -> Tuple[Decimal, Decimal]:
    Q96 = Decimal(2) ** 96
    sqrt_price = Decimal(sqrt_price_x96) / Q96

    # Uniswap V3 中 sqrt_price 是 token1/token0 的平方根
    price_token1_per_token0 = sqrt_price ** 2

    # 考虑精度调整（更稳健的写法）
    if token0_decimals >= token1_decimals:
        decimal_adjust = Decimal(10) ** (token0_decimals - token1_decimals)
    else:
        decimal_adjust = Decimal(1) / (Decimal(10) ** (token1_decimals - token0_decimals))
    price_token1_per_token0 *= decimal_adjust

    # 计算另一方价格
    if token0_usd is not None:
        token0_usd = Decimal(str(token0_usd))
        token1_usd = token0_usd * price_token1_per_token0
    elif token1_usd is not None:
        token1_usd = Decimal(str(token1_usd))
        token0_usd = token1_usd / price_token1_per_token0
    else:
        raise ValueError("必须提供 token0_usd 或 token1_usd 中的一个")

    # 保留18位小数并处理异常
    try:
        token0_usd = token0_usd.quantize(Decimal('1.000000000000000000'), rounding=ROUND_DOWN)
    except InvalidOperation:
        token0_usd = Decimal('0')

    try:
        token1_usd = token1_usd.quantize(Decimal('1.000000000000000000'), rounding=ROUND_DOWN)
    except InvalidOperation:
        token1_usd = Decimal('0')

    return token0_usd, token1_usd
