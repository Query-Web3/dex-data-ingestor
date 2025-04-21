import configparser

class MultiEnvDBConfig:
    def __init__(self, config_file="config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        if 'database' not in self.config:
            raise ValueError("Missing [database] section in config.")

        self.base = self.config['database']

    def get_config(self, env_name):
        if env_name not in self.config:
            raise ValueError(f"Missing [{env_name}] section in config.")
        env_config = self.config[env_name]
        merged = {**self.base, **env_config}
        merged['port'] = int(merged['port'])  # 类型转换
        return merged
