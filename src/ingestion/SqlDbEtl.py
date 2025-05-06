import mysql.connector
import logging
from datetime import datetime, timedelta, timezone

# 日志配置
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO
)


class SQL_DB_ETL:
    """
    通用 ETL 调度器，支持多任务区分，记录每个任务的 last_run
    """
    def __init__(self, local_config, remote_config):
        self.local_cfg = local_config
        self.remote_cfg = remote_config
        self.initialize_control_table()

    def initialize_control_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS etl_control (
            task_name VARCHAR(100) PRIMARY KEY,
            last_run DATETIME
        );
        """
        self.execute_sql(sql, use_remote=False)

    # def execute_sql(self, query, params=None, fetch=False, use_remote=False):
    #     cfg = self.remote_cfg if use_remote else self.local_cfg
    #     cnx = mysql.connector.connect(**cfg)
    #     cursor = cnx.cursor()
    #     try:
    #         if params:
    #             cursor.execute(query, params)
    #         else:
    #             cursor.execute(query)
    #         result = None
    #         if fetch:
    #             result = cursor.fetchall()
    #         cnx.commit()
    #         return result
    #     finally:
    #         cursor.close()
    #         cnx.close()

    def execute_sql(self, query, params=None, fetch=False, use_remote=False):
        cfg = self.remote_cfg if use_remote else self.local_cfg
        cnx = mysql.connector.connect(**cfg)
        cursor = cnx.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            result = None
            if fetch:
                result = cursor.fetchall()
            elif cursor.with_rows:
                # 强制清空结果集，避免 "Unread result found"
                cursor.fetchall()

            cnx.commit()
            return result
        finally:
            cursor.close()
            cnx.close()
    
    def get_last_run(self, task_name):
        rows = self.execute_sql(
            "SELECT last_run FROM etl_control WHERE task_name=%s",
            (task_name,), fetch=True, use_remote=False
        )
        if rows and rows[0][0]:
            dt = rows[0][0]
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        return None

    def set_last_run(self, task_name, dt):
        self.execute_sql(
            "REPLACE INTO etl_control (task_name, last_run) VALUES (%s, %s)",
            (task_name, dt), use_remote=False
        )

    def etl_job_till_now(self, task_fn, task_name):
        """
        通用调度入口：
        - 获取上次运行时间 last_run
        - 计算本次 end_time（当前整点）
        - 调用 task_fn(last_run, end_time) 执行业务
        - 根据返回值更新 last_run
        """
        now = datetime.now(timezone.utc)
        end_time = now.replace(minute=0, second=0, microsecond=0)
        last_run = self.get_last_run(task_name)

        # 调用任务函数
        count, new_last = task_fn(last_run, end_time)

        # 更新 last_run
        if new_last and new_last > (last_run or datetime.min.replace(tzinfo=timezone.utc)):
            self.set_last_run(task_name, new_last)
            logging.info(f"[{task_name}] 处理 {count} 条，last_run 更新至 {new_last}")
        else:
            logging.info(f"[{task_name}] 无新数据或 last_run 不变")

    def etl_job(self, task_fn, task_name, last_run, end_time):
        """
        通用调度入口：
        - 调用 task_fn(last_run, end_time) 执行业务
        - 根据返回值更新 last_run
        """
        
        # 调用任务函数
        count, new_last = task_fn(last_run, end_time)

        # 更新 last_run
        if new_last and new_last > (last_run or datetime.min.replace(tzinfo=timezone.utc)):
            self.set_last_run(task_name, new_last)
            logging.info(f"[{task_name}] 处理 {count} 条，last_run 更新至 {new_last}")
        else:
            logging.info(f"[{task_name}] 无新数据或 last_run 不变")
            

    # 任务示例：同步 dim_chains
    def sync_bifrost_to_dim_chains_task(self, last_run, end_time):
        if not last_run:
            row = self.execute_sql(
                "SELECT MIN(created_at) FROM Bifrost_batchID_table",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_chains] 无远程数据，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        rows = self.execute_sql(
            """
            SELECT DISTINCT chain FROM Bifrost_batchID_table
            WHERE created_at > %s AND created_at <= %s
            """,
            (last_run, end_time), fetch=True, use_remote=True
        )
        count = 0
        for (chain_name,) in rows:
            res = self.execute_sql(
                "SELECT chain_id FROM dim_chains WHERE name=%s",
                (chain_name,), fetch=True, use_remote=False
            )
            if not res:
                logging.warning(f"[sync_dim_chains] 未找到 dim_chains: {chain_name}")
                continue
            chain_id = res[0][0]
            self.execute_sql(
                """
                INSERT INTO dim_chains (name, chain_id)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                  chain_id=VALUES(chain_id), updated_at=NOW()
                """,
                (chain_name, chain_id), use_remote=False
            )
            count += 1
        return count, end_time

    # 任务示例：同步 dim_tokens
    def sync_stellar_dim_tokens_task(self, last_run, end_time):
        if not last_run:
            row = self.execute_sql(
                "SELECT MIN(created_at) FROM pool_data",
                fetch=True, use_remote=True
            )[0][0]
            if not row:
                logging.info("[sync_dim_tokens] 无远程 pool_data，退出")
                return 0, None
            last_run = row - timedelta(seconds=1)

        rows = self.execute_sql(
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
            chain_name = 'Stellar'
            res = self.execute_sql(
                "SELECT chain_id FROM dim_chains WHERE name=%s",
                (chain_name,), fetch=True, use_remote=False
            )
            if not res:
                logging.warning(f"[sync_stellar_dim_tokens] 未找到 dim_chains: {chain_name}")
                continue
            chain_id = res[0][0]
            asset_type_id = 1
            # token0
            if tok0_id and tok0_id not in processed:
                self.execute_sql(
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
                self.execute_sql(
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
