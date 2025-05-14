from gzip import FEXTRA
import logging;
from datetime import timedelta
from re import T
from ingestion.SqlDbEtl import SQL_DB_ETL
from utils.utils import calculate_tvl, calculate_yoy, calculate_qoq, last_year, last_quarter, prepare_apy_for_sql

class Stellar:

    def __init__(self, sql_db: SQL_DB_ETL):
        self.ChainName = 'stellaswap'
        self.SqlDb = sql_db
    
    # 任务示例：同步 dim_tokens
    def sync_stellar_dim_tokens_task(self, last_run, end_time):
        if not last_run:
            row = self.SqlDb.execute_sql(
                "SELECT MIN(created_at) FROM pool_data",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_stellar_dim_tokens] 无远程 pool_data，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        rows = self.SqlDb.execute_sql(

            """
            SELECT p.token0_id,p.token0_symbol,p.token0_name,p.token0_decimals,
                   p.token1_id,p.token1_symbol,p.token1_name,p.token1_decimals,
                   p.volume_usd_current, p.pool_id, p.tx_count, p.amount_token0, p.amount_token1, 
                   p.sqrt_price, p.final_apr, p.created_at
            FROM pool_data p
            WHERE p.created_at > %s AND p.created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )
        processed = set()
        return_type_id = 1

        fact_token_daily_stats_processed = set()
        fact_yield_stats_processed = set()

        for token0_id,token0_symbol,token0_name,token0_decimals,token1_id,token1_symbol,token1_name,token1_decimals,volume_usd_current,pool_id,tx_count,amount_token0, amount_token1, sqrt_price,final_apr,created_at in rows:
            res = self.SqlDb.execute_sql(
                "SELECT chain_id FROM dim_chains WHERE name=%s",
                (self.ChainName,), fetch=True, use_remote=False
            )
            if not res:
                logging.warning(f"[sync_stellar_dim_tokens] 未找到 dim_chains: {self.ChainName}")
                continue
            chain_id = res[0][0]

            asset_type_id = 1
            date = created_at.date()

            # 计算上季度和去年同期日期
            prev_quarter = last_quarter(date)
            prev_year = last_year(date)

            if not final_apr:
                apy = 0
            else:
                apy = prepare_apy_for_sql(final_apr/100, 365)
            
            # token0
            if token0_id and token0_id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id,token0_id,token0_symbol,token0_name,token0_decimals,asset_type_id), use_remote=False
                )
                logging.info(f"[sync_stellar_dim_tokens] 插入或更新 dim_tokens: {token0_id}")
                # 获取 token0_id
                tokenID0 = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, token0_id), fetch=True, use_remote=False
                )[0][0]
                logging.info(f"[sync_stellar_dim_tokens] 获取 token0_id: {tokenID0}")
                
                fact_token_daily_stats_key = (tokenID0, date)
                # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:

                # 获取 yoy
                res = self.SqlDb.execute_sql("""
                    SELECT volume, txns_count FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                """, (tokenID0, prev_year), fetch=True)
                volume_year = None
                txns_count_year = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_year = res[0][0]
                    txns_count_year = res[0][1]

                volume_yoy = calculate_yoy(volume_usd_current, volume_year)
                txns_count_yoy = calculate_yoy(tx_count, txns_count_year)
                    
                # 获取 qoq
                res = self.SqlDb.execute_sql("""
                    SELECT volume, txns_count FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                    """, (tokenID0, prev_quarter), fetch=True)

                volume_quarter = None
                txns_count_quarter = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_quarter = res[0][0]
                    txns_count_quarter = res[0][1]

                volume_qoq = None
                txns_count_qoq = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_qoq = calculate_qoq(volume_usd_current, volume_quarter)
                    txns_count_qoq = calculate_qoq(tx_count, txns_count_quarter)

                # 插入 fact_token_daily_stars We 
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_token_daily_stats
                    (token_id, date, volume, volume_usd, volume_yoy, volume_qoq, txns_count, txns_yoy, txns_qoq, price_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd), volume_usd = VALUES(volume_usd), 
                        volume_yoy = VALUES(volume_yoy), volume_qoq = VALUES(volume_qoq), txns_count = VALUES(txns_count),
                        txns_yoy = VALUES(txns_yoy), txns_qoq = VALUES(txns_qoq), created_at = VALUES(created_at)
                        """,
                        (tokenID0, date, volume_usd_current, volume_usd_current, volume_yoy, volume_qoq, tx_count, txns_count_qoq, txns_count_qoq, 0, created_at), use_remote=False
                    )
                logging.info(f"Swap sync_stellar_dim_tokens_task 插入 fact_token_daily_stats: {tokenID0}")
                fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
                ### endif

                ## add to fact_yield_stats
                fact_yield_stats_key = (tokenID0, token0_id, date)
                #if fact_yield_stats_key not in fact_yield_stats_processed:
                
                tvl = calculate_tvl(amount_token0, amount_token1, sqrt_price, token0_decimals, token1_decimals)
               
                logging.info(f"Swap sync_stellar_dim_tokens_task TVL: {tvl}")
                
                # 插入 fact_yield_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_yield_stats
                    (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE return_type_id = VALUES(return_type_id),apy = VALUES(apy), tvl = VALUES(tvl), tvl_usd = VALUES(tvl_usd)
                    """,
                    (tokenID0, return_type_id, pool_id, date, apy, tvl, tvl, created_at), use_remote=False
                )
                logging.info(f"Swap sync_stellar_dim_tokens_task 插入 fact_yield_stats: {tokenID0}")
                fact_yield_stats_processed.add(fact_yield_stats_key)
                #endif

                processed.add(token0_id)

            ###########################################
            # token1
            if token1_id and token1_id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id,token1_id,token1_symbol,token1_name,token1_decimals,asset_type_id), use_remote=False
                )
                logging.info(f"Swap sync_stellar_dim_tokens_task 插入或更新 dim_tokens: {token1_id}")

                   # 获取 token0_id
                tokenID1 = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, token1_id), fetch=True, use_remote=False
                )[0][0]
                logging.info(f"Swap sync_stellar_dim_tokens_task 获取 token1_id: {tokenID1}")

                # 获取 yoy
                res = self.SqlDb.execute_sql("""
                    SELECT volume, txns_count FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                """, (tokenID1, prev_year), fetch=True)
                volume_year = None
                txns_count_year = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_year = res[0][0]
                    txns_count_year = res[0][1]

                volume_yoy = calculate_yoy(volume_usd_current, volume_year)
                txns_count_yoy = calculate_yoy(tx_count, txns_count_year)
                    
                # 获取 qoq
                res = self.SqlDb.execute_sql("""
                    SELECT volume, txns_count FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                    """, (tokenID1, prev_quarter), fetch=True)
                

                volume_qoq = None
                txns_count_qoq = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_qoq = res[0][0]
                    txns_count_qoq = res[0][1]

                volume_qoq = calculate_qoq(volume_usd_current, volume_qoq)
                txns_count_qoq = calculate_qoq(tx_count, txns_count_qoq)

                fact_token_daily_stats_key = (tokenID1, date)
                # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
                # 插入 fact_token_daily_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_token_daily_stats
                    (token_id, date, volume, volume_usd, volume_yoy, volume_qoq, txns_count, txns_yoy, txns_qoq, price_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd), volume_usd = VALUES(volume_usd),
                    volume_yoy = VALUES(volume_yoy), volume_qoq = VALUES(volume_qoq), txns_count = VALUES(txns_count),
                    txns_yoy = VALUES(txns_yoy),txns_qoq = VALUES(txns_qoq), created_at = VALUES(created_at)
                    """,
                    (tokenID1, date, volume_usd_current, volume_usd_current, volume_yoy, volume_qoq, tx_count, txns_count_yoy, txns_count_qoq, 0, created_at), use_remote=False
                )
                logging.info(f"Swap sync_stellar_dim_tokens_task 插入 fact_token_daily_stats: {tokenID1}")
                fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
                ### endif

                ## add to fact_yield_stats
                fact_yield_stats_key = (tokenID1, token1_id, date)
                # if fact_yield_stats_key not in fact_yield_stats_processed:
                
                tvl = calculate_tvl(amount_token0, amount_token1, sqrt_price, token0_decimals, token1_decimals)
                # 插入 fact_yield_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_yield_stats
                    (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE return_type_id=VALUES(return_type_id),tvl = VALUES(tvl), apy = VALUES(apy), tvl_usd = VALUES(tvl_usd)
                    """,
                    (tokenID1, return_type_id, pool_id, date, apy, tvl, tvl, created_at), use_remote=False
                )
                logging.info(f"Swap sync_stellar_dim_tokens_task 插入 fact_yield_stats: {tokenID1}")
                fact_yield_stats_processed.add(fact_yield_stats_key)
                ### endif
                processed.add(token1_id)
        return len(processed), end_time