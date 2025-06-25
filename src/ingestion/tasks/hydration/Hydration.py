import logging;
from datetime import timedelta
from ingestion.SqlDbEtl import SQL_DB_ETL
from utils.utils import calculate_yoy, calculate_qoq, last_quarter, last_year, prepare_apy_for_sql
from zoneinfo import ZoneInfo

class Hydration:

    def __init__(self, sql_db: SQL_DB_ETL):
        self.ChainName = 'Hydration'
        self.SqlDb = sql_db
    
    def normalize_symbol(self,symbol: str) -> str:
        if symbol.lower().startswith('v'):
            return symbol
        return 'v' + symbol
    
    def sync_dim_tokens_hydration_price_task(self, last_run, end_time):
        if not last_run:
            row = self.SqlDb.execute_sql(
                "SELECT MIN(created_at) FROM Hydration_price",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_tokens_hydration_price] 无远程 Hydration_price，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        rows = self.SqlDb.execute_sql(
            """
            SELECT p.id,p.batch_id,p.asset_id,p.symbol,p.price_usdt,p.created_at
            FROM Hydration_price p
            WHERE p.created_at > %s AND p.created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )

        res = self.SqlDb.execute_sql(
            "SELECT chain_id FROM dim_chains WHERE name=%s",
            (self.ChainName,), fetch=True, use_remote=False
        )
        if not res:
            logging.warning(f"[sync_dim_tokens_hydration_price_task] 未找到 dim_chains: {self.ChainName}")
            return 0, None
        chain_id = res[0][0]

        processed = set()
        fact_token_daily_stats_processed = set()
        count = 0

        for id,batch_id,asset_id,symbol,price_usdt,created_at in rows:
            asset_type_id = 1
            if id and id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id, symbol, symbol, symbol, 18, asset_type_id), use_remote=False
                )
                logging.info(f"Hydration sync_dim_tokens_hydration_price_task 插入或更新 dim_tokens: {symbol}")
                
                token_id = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, symbol), fetch=True, use_remote=False
                )[0][0]
 
                # res2 = self.SqlDb.execute_sql(
                #     "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                #     (chain_id1, self.normalize_symbol(symbol)), fetch=True, use_remote=False
                # )
                # if res2 and len(res2) > 0 and res2[0][0] is not None:
                #     token_id1 = res2[0][0]

                date = created_at.date()
                fact_token_daily_stats_key = (token_id, date)
                # fact_token_daily_stats_key1 = (token_id1, date)
                # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
                # 插入 fact_token_daily_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_token_daily_stats
                    (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                    VALUES (%s, %s, 0, 0, 0, %s, %s)
                    ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd)
                    """,
                    (token_id, date, price_usdt, created_at), use_remote=False
                )
                # self.SqlDb.execute_sql(
                #     """
                #     INSERT INTO fact_token_daily_stats
                #     (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                #     VALUES (%s, %s, 0, 0, 0, %s, %s)
                #     ON DUPLICATE KEY UPDATE volume = VALUES(volume), volume_usd = VALUES(volume_usd), 
                #     txns_count = VALUES(txns_count), price_usd = VALUES(price_usd)
                #     """,
                #     (token_id1, date, price_usdt, created_at), use_remote=False
                # )
                logging.info(f"Hydration sync_dim_tokens_hydration_price_task 插入 fact_token_daily_stats: {token_id}")
                fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
                count += 1
                #endif
                processed.add(id)


        return count, end_time
    
    def sync_dim_tokens_hydration_data_task(self, last_run, end_time):
        if not last_run:
            row = self.SqlDb.execute_sql(
                "SELECT MIN(created_at) FROM hydration_data",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_tokens_hydration_data] 无远程 Hydration_data，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        rows = self.SqlDb.execute_sql(
            """
            SELECT p.id,p.batch_id,p.asset_id,p.symbol,p.farm_apr, p.pool_apr, p.total_apr, p.tvl_usd,
            p.volume_usd, p.timestamp, p.created_at
            FROM hydration_data p
            WHERE p.created_at > %s AND p.created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )

        res = self.SqlDb.execute_sql(
            "SELECT chain_id FROM dim_chains WHERE name=%s",
            (self.ChainName,), fetch=True, use_remote=False
        )
        if not res:
            logging.warning(f"[sync_dim_tokens_hydration_data_task] 未找到 dim_chains: {self.ChainName}")
            return 0, None
        chain_id = res[0][0]


        # res = self.SqlDb.execute_sql(
        #     "SELECT chain_id FROM dim_chains WHERE name=%s",
        #     ('Bifrost',), fetch=True, use_remote=False
        # )
        # if not res:
        #     logging.warning(f"[sync_dim_tokens_hydration_price_task] 未找到 dim_chains: {self.ChainName}")
        #     return 0, None
        # chain_id1 = res[0][0]
        

        # chain_id = 3
        fact_token_daily_stats_processed = set()
        fact_yield_stats_processed = set()
        processed = set()
        asset_type_id = 1
        decimals = 18
        for id,batch_id,asset_id,symbol,farm_apr,pool_apr,total_apr,tvl_usd,volume_usd,timestamp,created_at in rows:         
            if id and id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id, symbol, symbol, symbol, decimals, asset_type_id), use_remote=False
                )
                logging.info(f"Hydration sync_dim_tokens_hydration_data_task 插入或更新 dim_tokens: {symbol}")

                # self.SqlDb.execute_sql(
                #     """
                #     INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                #     VALUES (%s,%s,%s,%s,%s,%s)
                #     ON DUPLICATE KEY UPDATE
                #       symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                #       asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                #     """,
                #     (chain_id1, symbol, symbol, symbol, decimals, asset_type_id), use_remote=False
                # )
                # logging.info(f"Hydration sync_dim_tokens_hydration_data_task 插入或更新 dim_tokens: {symbol}")

                token_id = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, symbol), fetch=True, use_remote=False 
                )[0][0]

                # res2 = self.SqlDb.execute_sql(
                #     "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                #     (chain_id1, self.normalize_symbol(symbol)),
                #     fetch=True,
                #     use_remote=False
                # )
                # token_id1 = res2[0][0] if res2 else None

                date = created_at.date() 
                # 计算上季度和去年同期日期
                prev_quarter = last_quarter(date)
                prev_year = last_year(date)

                print("prev_quarter", prev_quarter)
                print("prev_year", prev_year)
                
                fact_token_daily_stats_key = (token_id, date)
                # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
                # 获取 yoy
                res = self.SqlDb.execute_sql("""
                    SELECT volume FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                    """, (token_id, prev_year), fetch=True, use_remote=False)

                volume_year = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_year = res[0][0]
                volume_yoy = calculate_yoy(volume_usd, volume_year)
                
                # 获取 qoq
                res = self.SqlDb.execute_sql("""
                    SELECT volume FROM fact_token_daily_stats 
                    WHERE token_id = %s AND date = %s
                    """, (token_id, prev_quarter), fetch=True, use_remote=False)

                volume_quarter = None
                if res and len(res) > 0 and res[0][0] is not None :
                    volume_quarter = res[0][0]

                volume_qoq = calculate_qoq(volume_usd, volume_quarter)
                
                # 插入 fact_token_daily_stats  price_usd参考hydration_price
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_token_daily_stats
                    (token_id, date, volume, volume_usd, volume_yoy, volume_qoq, txns_count, price_usd,created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE volume_usd = VALUES(volume_usd), volume = VALUES(volume),
                    volume_yoy = VALUES(volume_yoy), volume_qoq = VALUES(volume_qoq),
                    created_at = VALUES(created_at)
                    """,
                    (token_id, date, volume_usd, volume_usd, volume_yoy, volume_qoq, 0, 0, created_at), use_remote=False
                )
                logging.info(f"Hydration sync_dim_tokens_hydration_data_task 插入 fact_token_daily_stats: {token_id}")
                fact_token_daily_stats_processed.add(fact_token_daily_stats_key)

                # if token_id1:
                #     # 获取 yoy
                #     res = self.SqlDb.execute_sql("""
                #         SELECT volume FROM fact_token_daily_stats 
                #         WHERE token_id = %s AND date = %s
                #         """, (token_id1, prev_year), fetch=True, use_remote=False)

                #     volume_year1 = None
                #     if res and len(res) > 0 and res[0][0] is not None :
                #         volume_year1 = res[0][0]
                        
                #     volume_yoy1 = calculate_yoy(volume_usd, volume_year1)
                    
                #     # 获取 qoq
                #     res = self.SqlDb.execute_sql("""
                #         SELECT volume FROM fact_token_daily_stats 
                #         WHERE token_id = %s AND date = %s
                #         """, (token_id1, prev_quarter), fetch=True, use_remote=False)
                        
                #     volume_quarter1 = None
                #     if res and len(res) > 0 and res[0][0] is not None :
                #         volume_quarter1 = res[0][0]
                        
                #     volume_qoq1 = calculate_qoq(volume_usd, volume_quarter1)

                #     # 插入 fact_token_daily_stats
                #     self.SqlDb.execute_sql(
                #         """
                #         INSERT INTO fact_token_daily_stats
                #         (token_id, date, volume, volume_usd, volume_yoy, volume_qoq, txns_count, price_usd, created_at)
                #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                #         ON DUPLICATE KEY UPDATE volume_usd = VALUES(volume_usd), volume = VALUES(volume),
                #         volume_yoy = VALUES(volume_yoy), volume_qoq = VALUES(volume_qoq),
                #         created_at = VALUES(created_at) 
                #         """,
                #         (token_id1, date, volume_usd, volume_usd, volume_yoy1, volume_qoq1, 0, 0, created_at), use_remote=False
                #     )
                #     logging.info(f"Hydration sync_dim_tokens_hydration_data_task 插入 fact_token_daily_stats: {token_id1}")
                #     #endif
                
                # add yield stats
                pool_address = asset_id
                fact_yield_stats_key = (token_id, pool_address, date)
                # if fact_yield_stats_key not in fact_yield_stats_processed:
                n = 365
                if not total_apr:
                    apy = 0
                else:
                    apy = prepare_apy_for_sql(total_apr/100, n)
                return_type_id = 2
                tvl = 0
                # 插入 fact_yield_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_yield_stats
                    (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE apy = VALUES(apy), tvl = VALUES(tvl), pool_address = VALUES(pool_address),
                    tvl_usd = VALUES(tvl_usd)
                    """,
                    (token_id, return_type_id, pool_address, date, apy or 0, tvl_usd or 0, tvl_usd or 0, created_at), use_remote=False
                )

                #  # 插入 fact_yield_stats farm_apr
                # self.SqlDb.execute_sql(
                #     """
                #     INSERT INTO fact_yield_stats
                #     (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                #     ON DUPLICATE KEY UPDATE apy = VALUES(apy), tvl = VALUES(tvl), pool_address = VALUES(pool_address),
                #     tvl_usd = VALUES(tvl_usd)
                #     """,
                #     (token_id, 2, pool_address, date, farm_apr or 0, tvl_usd or 0, tvl_usd or 0, created_at), use_remote=False
                # )
                logging.info(f"Hydration sync_dim_tokens_hydration_data_task 插入 fact_yield_stats: {token_id}")
                fact_yield_stats_processed.add(fact_yield_stats_key)
                #endif
                processed.add(id)
        return len(fact_token_daily_stats_processed) + len(fact_yield_stats_processed), end_time
        