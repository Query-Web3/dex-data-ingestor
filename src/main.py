import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from ingestion.SqlDbEtl import SQL_DB_ETL
from ingestion.tasks.bifrost.Bifrost import Bifrost
from ingestion.tasks.bifrost.stellar.Stellar import Stellar
from ingestion.tasks.hydration.Hydration import Hydration
from datetime import datetime, timezone
from config.MultiEnvDBConfig import MultiEnvDBConfig

# 日志配置
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO
)

if __name__ == "__main__":

    cfg_loader = MultiEnvDBConfig("config.ini")
    local_config = cfg_loader.get_config("local")
    remote_config = cfg_loader.get_config("remote")
    sql_db = SQL_DB_ETL(local_config, remote_config)

    # local_config = {
    #     'user':'root','password':'aA923685261','host':'127.0.0.1','port':3306,'database':'queryweb3'
    # }
    # remote_config = {
    #     'user':'jtyd_db_user','password':'dfsw33DG3!','host':'194.233.75.155','port':3306,'database':'QUERYWEB3'
    # }
    # sql_db = SQL_DB_ETL(local_config, remote_config)

    # ten_days_ago = datetime.now(timezone.utc) - timedelta(days=10)
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime.now(timezone.utc)

    # 立即执行两次
    # logging.info("立即执行 sync_bifrost_to_fact_yield_stats_task")
    # sql_db.etl_job(Bifrost(sql_db).sync_bifrost_to_fact_yield_stats_task, 'sync_bifrost_to_fact_yield_stats_task', start_time, end_time)
    
    logging.info("立即执行 sync_dim_tokens_apy_from_site_task")
    sql_db.etl_job(Bifrost(sql_db).sync_dim_tokens_apy_from_site_task, 'sync_dim_tokens_apy_from_site_task', start_time, end_time)
    logging.info("立即执行 sync_dim_tokens_apy_from_staking_task")
    sql_db.etl_job(Bifrost(sql_db).sync_dim_tokens_apy_from_staking_task, 'sync_dim_tokens_apy_from_staking_task', start_time, end_time)
    
    logging.info("立即执行 sync_stellar_dim_tokens_task")
    sql_db.etl_job(Stellar(sql_db).sync_stellar_dim_tokens_task, 'sync_stellar_dim_tokens_task', start_time, end_time)
    
    logging.info("立即执行 sync_dim_tokens_hydration_price_task")
    sql_db.etl_job(Hydration(sql_db).sync_dim_tokens_hydration_price_task, 'sync_dim_tokens_hydration_price_task', start_time, end_time)
    logging.info("立即执行 sync_dim_tokens_hydration_data_task")
    sql_db.etl_job(Hydration(sql_db).sync_dim_tokens_hydration_data_task, 'sync_dim_tokens_hydration_data_task', start_time, end_time)

    # 定时调度
    scheduler = BlockingScheduler(timezone="UTC")
    # scheduler.add_job(sql_db.etl_job, 'cron', minute=0, args=[Bifrost(sql_db).sync_bifrost_to_fact_yield_stats_task, 'sync_bifrost_to_fact_yield_stats_task'])
    scheduler.add_job(sql_db.etl_job_till_now, 'cron', minute=0, args=[Bifrost(sql_db).sync_dim_tokens_apy_from_site_task, 'sync_dim_tokens_apy_from_site_task'])
    scheduler.add_job(sql_db.etl_job_till_now, 'cron', minute=0, args=[Bifrost(sql_db).sync_dim_tokens_apy_from_staking_task, 'sync_dim_tokens_apy_from_staking_task'])
    
    scheduler.add_job(sql_db.etl_job_till_now, 'cron', minute=0, args=[Stellar(sql_db).sync_stellar_dim_tokens_task, 'sync_stellar_dim_tokens_task'])
    
    scheduler.add_job(sql_db.etl_job_till_now, 'cron', minute=0, args=[Hydration(sql_db).sync_dim_tokens_hydration_price_task, 'sync_dim_tokens_hydration_price_task'])
    scheduler.add_job(sql_db.etl_job_till_now, 'cron', minute=0, args=[Hydration(sql_db).sync_dim_tokens_hydration_data_task, 'sync_dim_tokens_hydration_data_task']) 
    
    logging.info("调度启动：每小时整点运行同步任务")
    scheduler.start()
