__author__ = 'james'

import ConfigParser
from Logging import Logging


class Configs:
    """
        handle configurations
    """
    CONFIG_FILE = 'bsrs.cfg'
    Logging = Logging()

    def __init__(self, config_file=CONFIG_FILE, logging=Logging):
        self.config_file = config_file
        self.logging = logging
        self.config = ConfigParser.ConfigParser()
        self.load_config_file()

    def load_config_file(self):
        """
            Load config file
        """
        try:
            self.config.read(self.config_file)
            info_msg = "Loaded config file %s" % self.config_file
            self.logging.write_log('fetcher', 'i', info_msg)
            return self.config
        except Exception, e:
            info_msg = "config file %s missing" % self.config_file
            self.logging.write_log('fetcher', 'e', info_msg)
            raise Exception(info_msg)

    def get_db_creds(self):
        """
            load db creds from config file
        """
        hostname = self.config.get('database', 'db_hostname')
        username = self.config.get('database', 'db_username')
        passwd = self.config.get('database', 'db_password')
        db_name = self.config.get('database', 'db_dbname')
        return {'hostname': hostname, 'username': username,
                'passwd': passwd, 'db_name': db_name}
