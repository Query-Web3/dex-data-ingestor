import logging;
from datetime import timedelta
from ingestion.SqlDbEtl import SQL_DB_ETL
from utils.utils import calculate_tvl

class Stellar:

    def __init__(self, sql_db: SQL_DB_ETL):
        self.ChainName = 'Bifrost'
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
            SELECT p.token0_id,p.token0_symbolbol,p.token0_name,p.token0_decimals,
                   p.token1_id,p.token1_symbolbol,p.token1_name,p.token1_decimals,
                   p.volume_usd_current, p.pool_id, p.tx_count, p.amount_token0, p.amount_token1, 
                   p.sqrt_price, p.created_at
            FROM pool_data p
            WHERE p.created_at > %s AND p.created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )
        processed = set()
        return_type_id = 1

        fact_token_daily_stats_processed = set()
        fact_yield_stats_processed = set()

        for token0_id,token0_symbol,token0_name,token0_decimals,token1_id,token1_symbol,token1_name,token1_decimals,volume_usd_current,pool_id,amount_token0, amount_token1, sqrt_price,created_at in rows:
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
                # 获取 token0_id
                tokenID0 = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, token0_id), fetch=True, use_remote=False
                )[0][0]

                fact_token_daily_stats_key = (tokenID0, date)
                if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
                    # 插入 fact_token_daily_stats
                    self.SqlDb.execute_sql(
                        """
                        INSERT INTO fact_token_daily_stats
                        (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                        VALUES (%s, %s, 0, %s, 0, 0, %s)
                        ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd), volume_usd = VALUES(volume_usd)
                        """,
                        (tokenID0, date, volume_usd_current, created_at), use_remote=False
                    )
                    fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
                
                ## add to fact_yield_stats
                fact_yield_stats_key = (tokenID0, token0_id, date)
                if fact_yield_stats_key not in fact_yield_stats_processed:
                
                    tvl = calculate_tvl(amount_token0, amount_token1, sqrt_price)
                    # 插入 fact_yield_stats
                    self.SqlDb.execute_sql(
                        """
                        INSERT INTO fact_yield_stats
                        (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE tvl = VALUES(tvl), apy = VALUES(apy), tvl_usd = VALUES(tvl_usd)
                        """,
                        (tokenID0, return_type_id, pool_id, date, 0, tvl or 0, 0, created_at), use_remote=False
                    )
                    fact_yield_stats_processed.add(fact_yield_stats_key)
                
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

                   # 获取 token0_id
                tokenID1 = self.SqlDb.execute_sql(
                    "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                    (chain_id, token1_id), fetch=True, use_remote=False
                )[0][0]

                fact_token_daily_stats_key = (tokenID1, date)
                if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
                    # 插入 fact_token_daily_stats
                    self.SqlDb.execute_sql(
                        """
                        INSERT INTO fact_token_daily_stats
                        (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                        VALUES (%s, %s, 0, %s, 0, 0, %s)
                        ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd), volume_usd = VALUES(volume_usd)
                        """,
                        (tokenID1, date, volume_usd_current, created_at), use_remote=False
                    )
                    fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
                
                ## add to fact_yield_stats
                fact_yield_stats_key = (tokenID1, token1_id, date)
                if fact_yield_stats_key not in fact_yield_stats_processed:
                
                    tvl = calculate_tvl(amount_token0, amount_token1, sqrt_price)
                    # 插入 fact_yield_stats
                    self.SqlDb.execute_sql(
                        """
                        INSERT INTO fact_yield_stats
                        (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE tvl = VALUES(tvl), apy = VALUES(apy), tvl_usd = VALUES(tvl_usd)
                        """,
                        (tokenID1, return_type_id, pool_id, date, 0, tvl or 0, 0, created_at), use_remote=False
                    )
                    fact_yield_stats_processed.add(fact_yield_stats_key)
                
                processed.add(token1_id)
        return len(processed), end_time