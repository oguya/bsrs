__author__ = 'james'

import os
import time
import Config

class Logging():
    """
        performs all logging tasks
    """

    DEFAULT_LOG_DIR = '/home/james/python-include/BSRS/logs/'
    LOG_FILE = {'fingerprint': DEFAULT_LOG_DIR+'fingerprint_logs.txt',
                'search': DEFAULT_LOG_DIR+'search_logs.txt',
                'fetcher': DEFAULT_LOG_DIR+'fetcher_logs.txt',
                'parsers': DEFAULT_LOG_DIR+'parsers_logs.txt',
                'databases': DEFAULT_LOG_DIR+'database_logs.txt'}

    LOG_TAG = {'e': 'ERROR', 'i': 'INFO', 'w': 'WARN', 'f': 'FATAL'}
    OVERWRITE = False

    def __init__(self,
                 log_dir=DEFAULT_LOG_DIR,
                 overwrite=OVERWRITE):

        self.log_dir = log_dir
        self.overwrite = overwrite

        #load configs
        #config = Config.Configs()
        #self.config = config.config
        #use_proxy = self.config.getboolean('Proxies', 'use_http_proxy')

        self.check_dirs()

    def check_dirs(self):
        if not os.path.exists(self.log_dir) and not os.path.isdir(self.log_dir):
            print "Log dir %s doesn't exist. Creating it" % self.log_dir
            os.mkdir(self.log_dir, 0755)
        else:
            print "log dir %s exists. proceed..." % self.log_dir

    def write_log(self, log_file='fingerprint', log_tag='i', log_msg=None):
        """
            write log to log file
            syntax -> [2013-12-07 22:37:05] [INFO] log msg
        """
        fh = self.open_log_file(log_file=log_file)
        log_txt = "[%s] [%s] %s\n" % (Logging.timestamp(), self.LOG_TAG[log_tag], log_msg)
        fh.write(log_txt)
        fh.flush()

    def open_log_file(self, log_file='fingerprint'):
        """
            open log file -> ret. file handle obj
        """
        #os.chdir(self.log_dir)
        if not os.path.isfile(self.LOG_FILE[log_file]):
            open(self.LOG_FILE[log_file], 'w+').close()

        return open(self.LOG_FILE[log_file], 'w' if self.overwrite else 'a')

    @staticmethod
    def timestamp():
        """
            return current_timestamp -> YYYY-mm-dd HH:MM:SS
        """
        c = time.localtime()
        str = "%s-%s-%s %s:%s:%s" % (c.tm_year, c.tm_mon, c.tm_mday, c.tm_hour, c.tm_min, c.tm_sec)
        return str
