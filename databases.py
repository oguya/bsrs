__author__ = 'james'

from Logging import Logging

import MySQLdb as mysql
import MySQLdb.cursors as cursors


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
    IMAGES_TBL = "images"
    SOUNDS_TBL = "sounds"

    #sql stmts
    #TODO add queries for select,insert,drops,updates

    #inserts
    INSERT_FINGERPRINT = "insert into %s(birdID, hash, start_time) values('%%d','unhex(%%s)', '%%d')" % (FINGERPRINTS_TBL)
    INSERT_IMAGES = " INSERT INTO %s(birdID, imageURL, siteURL) VALUES ('%%d', '%%s','%%s')" % (IMAGES_TBL)
    INSERT_SOUNDS = "INSERT INTO %s(birdID, soundType, wavFile, soundURL) values" \
                    "('%%d','%%s','%%s', '%%s')" % (SOUNDS_TBL)
    INSERT_BIRDS = "INSERT INTO %s(englishName, genericName, specificName, Recorder, Location, Country, lat_lng, xenoCantoURL) "\
                   "values('%%s', '%%s', '%%s', '%%s', '%%s', '%%s', '%%s', '%%s')" % (BIRDS_TBL)

    #selects
    SELECT = ""
    SELECT_ALL = ""
    SELECT_SONG = ""
    SELECT_NUM_FINGERPRINTS = ""
    SELECT_ALL_BIRDS = "SELECT birdID, englishName, genericName, specificName, Recorder, Location, Country, " \
                       "lat_lng, xenoCantoURL from %s" %s (BIRDS_TBL)

    SELECT_UNIQUE_SONG_IDS = ""
    SELECT_SONGS = ""

    # update
    UPDATE_SONG_FINGERPRINTED = ""

    # delete
    DELETE_UNFINGERPRINTED = ""
    DELETE_ORPHANS = ""

    #list tables
    LIST_TABLES = "show tables"


    def __init__(self, hostname, username, password, database):

        self.logging = Logging()

        #connect
        try:
            self.connection = mysql.connect(host=hostname, user=username, passwd=password,
                                            db=database, cursorclass=cursors.DictCursor)
            self.connection.autocommit(False)
            self.cursor = self.connection.cursor()
            self.logging.write_log('databases', 'i', "successfully connected to DB. DB Version: %s" %
                                                     self.connection.get_server_info())
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
            return int(record['count'])
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_num_fingerprints()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], MySQLDatabases.SELECT_NUM_FINGERPRINTS)))

    def get_all_birds(self):
        """
            returns cursor containing info abt all birds
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_ALL_BIRDS)
            return self.cursor.fetchall()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_all_birds()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], MySQLDatabases.SELECT_ALL_BIRDS)))
            raise Exception(e.message)


    def insert_images(self, birdID, imageURL, siteURL):
        """
            insert image urls to images table
        """
        sql = None
        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_IMAGES % (birdID, imageURL, siteURL)
            self.cursor.execute(sql)
            self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_images()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], sql)))
            return None

    def insert_birds(self, **kwargs):
        """
            insert bird info to birds table
            return birdID of the new insert
        """
        sql = None
        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_BIRDS % (
                kwargs.get('english_name'), kwargs.get('generic_name'), kwargs.get('specific_name'),
            kwargs.get('recorder'), kwargs.get('location'), kwargs.get('country'), kwargs.get('lat_lng'),
            kwargs.get('xeno_cantoURL'))
            self.cursor.execute(sql)
            self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_birds()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], sql)))
            return None

    def insert_sounds(self, birdID, soundType, wavFile, soundURL):
        sql = None
        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_SOUNDS % (birdID, soundType, wavFile, soundURL)
            self.cursor.execute(sql)
            self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_sounds()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], sql)))
            return None

    def insert_fingerprints(self, hashes, birdID):
        """
            insert a series of hash(songID, offset) values in db
        """
        for hash in hashes:
            sha1, val = hash
            self.insert(birdID, sha1, val)
        self.connection.commit()

    def insert(self, key, value, birdID):
        """
            insert a (sha1, songID, timeoffset) into MySQLdb
            key => sha1, value => (songID, timeoffset)
        """
        if birdID is not value[0]:
            print "birdID: %d is not the hashd value: %d "% (birdID, value[0])
        try:
            args = (value[0], key, value[1])
            self.cursor.execute(MySQLDatabases.INSERT_FINGERPRINT, args)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e',
                       ("{insert()} Query Error: %d: %s SQL: %s" % (e.args[0], e.args[1], (MySQLDatabases.INSERT_FINGERPRINT, args))))

