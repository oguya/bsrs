__author__ = 'james'

import MySQLdb as mysql
import MySQLdb.cursors as cursors

import Logging

class MySQLDatabases:

    #TODO add sql queries descriptions
    """
        Queries:
    """

    #db conn keys
    CONNECTION = "connection"
    KEY_USERNAME = "db_username"
    KEY_PASSWORD = "db_password"
    KEY_DATABASE = "db_name"
    KEY_HOST = "db_host"

    #tables to use
    FINGERPRINTS_TBL = "fingerprints"
    BIRDS_TBL = "birds"

    #sql stmts
    #TODO add queries for select,insert,drops,updates
    INSERT_FINGERPRINT = ""
    SELECT = ""
    SELECT_ALL = ""
    SELECT_SONG = ""
    SELECT_NUM_FINGERPRINTS = ""
    SELECT_NUM_FINGERPRINTS = ""

    SELECT_UNIQUE_SONG_IDS = ""
    SELECT_SONGS = ""

    # update
    UPDATE_SONG_FINGERPRINTED = ""

    # delete
    DELETE_UNFINGERPRINTED = ""
    DELETE_ORPHANS = ""

    #list tables
    LIST_TABLES = "show tables"

    #xtras
    CONFIG_FILE = 'bsrs.cfg'

    def __init__(self, hostname, username, password, database,
                 logging=Logging, config_file=CONFIG_FILE):

        self.logging = Logging()
        self.config_file = config_file

        #connect
        self.database = database
        try:
            self.connection = mysql.connect(user=username, passwd=password,
                                            database=database, cursorsclass=cursors.DictCursor)
            self.connection.autocommit(False)
            self.cursor = self.connection.cursor()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("Connection error %d: %s" % (e.args[0], e.args[1])))

    def setup(self):
        """
            drop existing tables and create new ones. BIO-HAZARD*****
        """
        #TODO use db sql file to drop tables

        pass


    def get_num_songs(self):
        """
            Return no of fingerprinted songs
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_UNIQUE_SONG_IDS)
            record = self.cursor.fetchone()
            return int(record['count'])
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], MySQLDatabases.SELECT_UNIQUE_SONG_IDS)))

    def get_num_fingerprints(self):
        """
            Returns no. of fingerprints in the db
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_NUM_FINGERPRINTS)
            record = self.cursor.fetchone()
            return  int(record['count'])
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], MySQLDatabases.SELECT_NUM_FINGERPRINTS)))