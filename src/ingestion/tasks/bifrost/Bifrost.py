import logging
from datetime import timedelta
from ingestion.SqlDbEtl import SQL_DB_ETL
from utils.utils import prepare_apy_for_sql
from zoneinfo import ZoneInfo

class Bifrost:

    def __init__(self, sql_db: SQL_DB_ETL):
        self.ChainName = 'Bifrost'
        self.SqlDb = sql_db

    # def sync_bifrost_to_fact_yield_stats_task(self, last_run, end_time):
    #     if not last_run:
    #         row = self.SqlDb.execute_sql(
    #             "SELECT MIN(created_at) FROM Bifrost_site_table",
    #             fetch=True, use_remote=True
    #         )[0][0]
    #         if not row:
    #             logging.info("[sync_bifrost_to_fact_yield_stats] 无远程数据，退出")
    #             return 0, None
    #         last_run = row - timedelta(seconds=1)

    #     res = self.SqlDb.execute_sql(
    #         "SELECT chain_id FROM dim_chains WHERE name=%s",
    #         (self.ChainName,), fetch=True, use_remote=False
    #     )
    #     if not res:
    #         logging.warning(f"[sync_bifrost_to_fact_yield_stats] 未找到 dim_chains: {self.ChainName}")
    #         return 0, None
    #     chain_id = res[0][0]

    #     rows = self.SqlDb.execute_sql(
    #         """
    #         SELECT batch_id, Asset, Value,tvl, tvm, holders, apy, apyBase, apyReward, totalIssuance, created_at FROM Bifrost_site_table
    #         WHERE created_at > %s AND created_at <= %s
    #         """,
    #         (last_run, end_time), fetch=True, use_remote=True
    #     )
    #     count = 0
    #     for (batch_id, asset, value, tvl, tvm, holders, apy, apyBase, apyReward, totalIssuance, created_at) in rows:
    #         date = created_at.date()
    #         self.SqlDb.execute_sql(
    #             """
    #             INSERT INTO fact_yield_stats (chain_id, date, apy, tvl, tvl_usd)
    #             VALUES (%s, %s, %s, %s, %s)
    #             ON DUPLICATE KEY UPDATE
    #               apy=VALUES(apy),
    #               tvl=VALUES(tvl),
    #               tvl_usd=VALUES(tvl_usd),
    #               updated_at=NOW()
    #             """,
    #             (chain_id, date, apy, tvl, 0), use_remote=False
    #         )
    #         count += 1
    #     return count, end_time

    def sync_dim_tokens_apy_from_site_task(self, last_run, end_time):

        """
        从远程 Bifrost_site_table 拉取新增 Asset，插入或更新 dim_tokens，
        并将 token_id 写入 fact_token_daily_stats
        固定 chain_id=4, asset_type_id=1
        """
        if not last_run:
            row = self.SqlDb.execute_sql(
                "SELECT MIN(created_at) FROM Bifrost_site_table",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_tokens_site] 无远程 site 数据，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)
        
        if not end_time:
            end_time = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        
        res = self.SqlDb.execute_sql(
            "SELECT chain_id FROM dim_chains WHERE name=%s",
            (self.ChainName,), fetch=True, use_remote=False
        )
        if not res:
            logging.warning(f"[sync_dim_tokens_site] 未找到 dim_chains: {self.ChainName}")
            return 0, None
        chain_id = res[0][0]
        logging.info(f"[sync_dim_tokens_site] last_run: {last_run}, end_time: {end_time}")
        
        rows = self.SqlDb.execute_sql(
            "SELECT Asset, tvl, tvm, holders, apy, apyBase, apyReward, totalIssuance, created_at FROM Bifrost_site_table "
            "WHERE created_at > %s AND created_at <= %s",
            (last_run, end_time), fetch=True, use_remote=True
        )
        fact_token_daily_stats_processed = set()
        fact_yield_stats_processed = set()
        count = 0
        count1 = 0
        for asset, tvl, tvm, holders, apy, apyBase, apyReward, totalIssuance, created_at in rows:
            if not asset:
                continue

            asset_lower = asset.lower()
            if asset_lower in {'tvl', 'addresses', 'revenue'}:
                continue

            # chain_id = 4
            asset_type_id = 1
            symbol = asset
            address = asset
            name = asset
            decimals = 18
            return_type_id = 1
            tvl_usd = 0
            date = created_at.date()

            # 插入或更新 dim_tokens
            self.SqlDb.execute_sql(
                """
                INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE symbol=VALUES(symbol),name=VALUES(name),
                decimals=VALUES(decimals),asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                """,
                (chain_id, address, symbol, name, decimals, asset_type_id), use_remote=False
            ) 
            logging.info(f"[sync_dim_tokens_site] 插入或更新 dim_tokens: {asset}, {chain_id}, {address}, {symbol}, {name}, {decimals}, {asset_type_id}")
            # 获取 token_id
            token_id = self.SqlDb.execute_sql(
                "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                (chain_id, address), fetch=True, use_remote=False
            )[0][0]
            logging.info(f"[sync_dim_tokens_site] 获取 token_id: {token_id}")

            price_usdt = 0  

            # 如果 symbol 以 v 开头，从 Hydration_price 表中获取 price_usdt,暂时注释掉，因为Bifrost_staking_table会覆盖
            # if symbol.startswith("v"):
            #     symbol = symbol[1:]
            #     rows = self.SqlDb.execute_sql(
            #         """
            #         SELECT p.price_usdt
            #         FROM Hydration_price p
            #         WHERE p.symbol = %s AND p.created_at > %s AND p.created_at <= %s
            #         """,
            #         (symbol, last_run, end_time), fetch=True, use_remote=True
            #     )
            #     if rows and len(rows) > 0 and rows[0][0] is not None:
            #         price_usdt = rows[0][0]
            


            fact_token_daily_stats_key = (token_id, date)
            # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
            # 插入 fact_token_daily_stats
            self.SqlDb.execute_sql(
                """
                INSERT INTO fact_token_daily_stats
                (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                VALUES (%s, %s, 0, 0, 0, %s, %s)
                ON DUPLICATE KEY UPDATE created_at = VALUES(created_at)
                """,
                (token_id, date, price_usdt, created_at), use_remote=False
            )
            logging.info(f"[sync_dim_tokens_site] 插入 fact_token_daily_stats: {token_id}")
            fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
            count += 1
        
            pool_address = ""
            fact_yield_stats_key = (token_id, pool_address, date)
            # if fact_yield_stats_key not in fact_yield_stats_processed:
            
            # 插入 fact_yield_stats
            self.SqlDb.execute_sql(
                """
                INSERT INTO fact_yield_stats
                (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE tvl = VALUES(tvl), apy = VALUES(apy), tvl_usd = VALUES(tvl_usd)
                """,
                (token_id, return_type_id, pool_address, date, apy or 0, tvl or 0, tvl or 0, created_at), use_remote=False
            )
            logging.info(f"[sync_dim_tokens_site] 插入 fact_yield_stats: {token_id}")
            fact_yield_stats_processed.add(fact_yield_stats_key)
            count1 += 1
        #endif
        return count + count1, end_time

    def sync_dim_tokens_apy_from_staking_task(self, last_run, end_time):
        """
        从远程 Bifrost_staking_table 拉取新增 symbol，插入或更新 dim_tokens
        """
        if not last_run:
            row = self.SqlDb.execute_sql(
                "SELECT MIN(created_at) FROM Bifrost_staking_table",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_tokens_staking] 无远程 staking 数据，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        res = self.SqlDb.execute_sql(
            "SELECT chain_id FROM dim_chains WHERE name=%s",
            (self.ChainName,), fetch=True, use_remote=False
        )
        if not res:
            logging.warning(f"[sync_dim_tokens_staking] 未找到 dim_chains: {self.ChainName}")
            return 0, None
        chain_id = res[0][0]

        rows = self.SqlDb.execute_sql(
            "SELECT symbol, contractAddress, apr, fee, price, exchangeRatio, supply, created_at FROM Bifrost_staking_table "
            "WHERE created_at > %s AND created_at <= %s",
            (last_run, end_time), fetch=True, use_remote=True
        )

        logging.info(f"[sync_dim_tokens_staking] 获取 staking 数据: {rows}")

        # chain_id = 4
        asset_type_id = 1
        decimals = 18
        return_type_id = 1
        tvl_usd = 0

        count = 0
        count1 = 0
        for symbol, contractAddress, apr, fee, price, exchangeRatio, supply, created_at  in rows:
            if not symbol:
                continue

            date = created_at.date()

            # 插入或更新 dim_tokens
            # 使用 symbol 作为 address
            self.SqlDb.execute_sql(
                """
                INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE symbol=VALUES(symbol),name=VALUES(name),
                decimals=VALUES(decimals),asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                """,
                (chain_id, symbol, symbol, symbol, decimals, asset_type_id), use_remote=False
            )

            logging.info(f"[sync_dim_tokens_staking] 插入或更新 dim_tokens: {symbol}")

            # 获取 token_id
            token_id = self.SqlDb.execute_sql(
                "SELECT id FROM dim_tokens WHERE chain_id=%s AND address=%s",
                (chain_id, symbol), fetch=True, use_remote=False
            )[0][0]
            logging.info(f"[sync_dim_tokens_staking] 获取 token_id: {token_id}")

            fact_token_daily_stats_processed = set()
            fact_token_daily_stats_key = (token_id, date)
            # if fact_token_daily_stats_key not in fact_token_daily_stats_processed:
            # 插入 fact_token_daily_stats
            self.SqlDb.execute_sql(
                """
                INSERT INTO fact_token_daily_stats
                (token_id, date, volume, volume_usd, txns_count, price_usd, created_at)
                VALUES (%s, %s, 0, 0, 0, %s, %s)
                ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd)
                """,
                (token_id, date, price * exchangeRatio, created_at), use_remote=False
            )
            logging.info(f"[sync_dim_tokens_staking] 插入 fact_token_daily_stats: {token_id}")
            fact_token_daily_stats_processed.add(fact_token_daily_stats_key)
            count += 1
        
            pool_address = ""
            fact_yield_stats_processed = set()
            fact_yield_stats_key = (token_id, pool_address, date)
            if fact_yield_stats_key not in fact_yield_stats_processed:
                n = 365
                if not apr:
                    apy = 0
                else:
                    apy = prepare_apy_for_sql(apr/100, n)
                
                if supply is None:
                    tvl = 0
                else:
                    tvl = supply * price
                tvl_usd = 0
                # 插入 fact_yield_stats
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO fact_yield_stats
                    (token_id, return_type_id, pool_address, date, apy, tvl, tvl_usd, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE apy = VALUES(apy), tvl = VALUES(tvl), tvl_usd = VALUES(tvl_usd),
                    return_type_id=VALUES(return_type_id)
                    """,
                    (token_id, return_type_id, pool_address, date, apy, tvl, tvl, created_at), use_remote=False
                )
                logging.info(f"[sync_dim_tokens_staking] 插入 fact_yield_stats: {token_id}")
                fact_yield_stats_processed.add(fact_yield_stats_key)
                count1 += 1

        return count + count1, end_time

