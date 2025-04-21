import logging;
from datetime import timedelta
from ingestion.SqlDbEtl import SQL_DB_ETL

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
            SELECT p.token0_id,p.token0_symbol,p.token0_name,p.token0_decimals,
                   p.token1_id,p.token1_symbol,p.token1_name,p.token1_decimals
            FROM pool_data p
            WHERE p.created_at > %s AND p.created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )
        processed = set()
        for tok0_id,tok0_sym,tok0_name,tok0_dec,tok1_id,tok1_sym,tok1_name,tok1_dec in rows:
            res = self.SqlDb.execute_sql(
                "SELECT chain_id FROM dim_chains WHERE name=%s",
                (self.ChainName,), fetch=True, use_remote=False
            )
            if not res:
                logging.warning(f"[sync_stellar_dim_tokens] 未找到 dim_chains: {self.ChainName}")
                continue
            chain_id = res[0][0]

            asset_type_id = 1

            # token0
            if tok0_id and tok0_id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id,tok0_id,tok0_sym,tok0_name,tok0_dec,asset_type_id), use_remote=False
                )
                processed.add(tok0_id)
            # token1
            if tok1_id and tok1_id not in processed:
                self.SqlDb.execute_sql(
                    """
                    INSERT INTO dim_tokens (chain_id,address,symbol,name,decimals,asset_type_id)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      symbol=VALUES(symbol),name=VALUES(name),decimals=VALUES(decimals),
                      asset_type_id=VALUES(asset_type_id),updated_at=NOW()
                    """,
                    (chain_id,tok1_id,tok1_sym,tok1_name,tok1_dec,asset_type_id), use_remote=False
                )
                processed.add(tok1_id)
        return len(processed), end_time