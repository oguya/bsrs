__author__ = 'james'

from Logging import Logging

import MySQLdb as mysql
import MySQLdb.cursors as cursors
from MySQLdb import escape_string as clean


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
    SOUNDS_TMP_TBL = "tmp_sounds"
    STATS_TBL = "stats"

    #tbl field names
    FIELD_BIRDNAME = "englishName"

    #sql stmts
    #TODO add queries for select,insert,drops,updates

    #inserts
    INSERT_FINGERPRINT = "INSERT INTO %s(birdID, hash, start_time) values(%%s,unhex(%%s), %%s)" % (FINGERPRINTS_TBL)
    INSERT_IMAGES = " INSERT INTO %s(birdID, imageURL, siteURL) VALUES ('%%s', '%%s','%%s')" % (IMAGES_TBL)
    INSERT_SOUNDS = "INSERT INTO %s(birdID, soundType, wavFile, soundURL) values" \
                    "('%%s','%%s','%%s', '%%s')" % (SOUNDS_TBL)
    INSERT_BIRDS = "INSERT INTO %s(englishName, genericName, specificName, Recorder, Location, Country, lat_lng, xenoCantoURL) "\
                   "values('%%s', '%%s', '%%s', '%%s', '%%s', '%%s', '%%s', '%%s')" % (BIRDS_TBL)
    INSERT_TMP_SOUNDS = "INSERT INTO %s(birdID, wavFile, soundType, soundURL) values('%%s', '%%s', '%%s', '%%s')" % (SOUNDS_TMP_TBL)
    INSERT_STATS = "insert into %s(birdID, match_time, confidence, offset) values(%%s, %%s, %%s, %%s)" % (STATS_TBL)

    #selects
    SELECT = "SELECT birdID, start_time FROM %s WHERE hash = UNHEX(%%s);" % (FINGERPRINTS_TBL)
    SELECT_ALL = "SELECT birdID, start_time FROM %s" % (FINGERPRINTS_TBL)

    SELECT_SOUNDS = "SELECT birdID, wavFile, fingerprinted FROM %s" % (SOUNDS_TBL)
    SELECT_NON_FINGERPRINTED_SOUNDS = "%s WHERE fingerprinted = 0" % (SELECT_SOUNDS)
    SELECT_NUM_FINGERPRINTS = "SELECT COUNT(*) AS fingerprints FROM %s" % (FINGERPRINTS_TBL)
    SELECT_ALL_BIRDS = "SELECT birdID, englishName, genericName, specificName, Recorder, Location, Country, " \
                       "lat_lng, xenoCantoURL from %s" % (BIRDS_TBL)
    SELECT_BIRD_BY_ID = "%s WHERE birdID = '%%s' " % (SELECT_ALL_BIRDS)
    SELECT_SOUND_BY_ID = "SELECT birdID, soundType, wavFile, soundURL FROM %s WHERE birdID = %%s" % (SOUNDS_TBL)

    SELECT_TMP_SOUNDS = "SELECT birdID, wavFile, soundType, soundURL FROM tmp_sounds ORDER BY 1 DESC"
    #SELECT_TMP_SOUNDS = "SELECT birdID, wavFile, soundType, soundURL FROM tmp_sounds WHERE birdID< '322' ORDER BY 1 DESC"

    # update
    UPDATE_SONG_FINGERPRINTED = "UPDATE %s SET fingerprinted=1 where birdID = '%%s'" % (SOUNDS_TBL)

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
            self.KEY_USERNAME = username
            self.KEY_DATABASE = database
            self.KEY_PASSWORD = password
            self.KEY_HOST = hostname

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
            self.logging.write_log('databases', 'e', ("Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], MySQLDatabases.SELECT_UNIQUE_SONG_IDS)))

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
            self.logging.write_log('databases', 'e', ("{get_num_fingerprints()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], MySQLDatabases.SELECT_NUM_FINGERPRINTS)))

    def get_all_birds(self):
        """
            returns cursor containing info abt all birds
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_ALL_BIRDS)
            return self.cursor.fetchall()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_all_birds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], MySQLDatabases.SELECT_ALL_BIRDS)))
            raise Exception(e.message)

    def get_bird_by_id(self, birdID):
        """
            return a cursor containing info about a bird
            with the given birdID
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_BIRD_BY_ID, birdID)
            return self.cursor.fetchone()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_bird_by_id()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], MySQLDatabases.SELECT_BIRD_BY_ID)))
            raise Exception(e.message)

    def get_sound_by_id(self, birdID):
        """
            return a cursor containing sound info
            given birdID
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_SOUND_BY_ID, birdID)
            return self.cursor.fetchone()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_bird_by_id()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], MySQLDatabases.SELECT_SOUND_BY_ID)))
            return None

    def insert_images(self, birdID, imageURL, siteURL):
        """
            insert image urls to images table
        """
        sql = None

        #cleaning inputs
        birdID = clean(str(birdID))
        imageURL = clean(imageURL)
        siteURL = clean(siteURL)

        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_IMAGES % (birdID, imageURL, siteURL)
            self.cursor.execute(sql)
            self.connection.autocommit(True)
            #self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_images()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))
            return None

    def insert_birds(self, **kwargs):
        """
            insert bird info to birds table
            return birdID of the new insert
        """
        sql = None
        try:
            self.cursor = self.connection.cursor()

            #clean inputs
            sql = MySQLDatabases.INSERT_BIRDS % (
                clean(kwargs.get('english_name')), clean(kwargs.get('generic_name')),
                clean(kwargs.get('specific_name')), clean(kwargs.get('recorder')),
                clean(kwargs.get('location')), clean(kwargs.get('country')),
                clean(kwargs.get('lat_lng')), clean(kwargs.get('xeno_cantoURL')))
            self.cursor.execute(sql)
            self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_birds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))
            return None

    def insert_sounds(self, birdID, soundType, wavFile, soundURL):
        sql = None
        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_SOUNDS % (clean(str(birdID)), clean(soundType), clean(wavFile), clean(soundURL))
            self.cursor.execute(sql)
            self.connection.commit()
            return int(self.cursor.lastrowid)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_sounds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))
            return None

    def insert_fingerprints(self, hashes, birdID):
        """
            insert a series of hash(songID, offset) values in db
        """
        for hash in hashes:
            sha1, val = hash
            self.insert(key=sha1, value=val, birdID=birdID)
        self.connection.commit()

    def insert(self, key, value, birdID):
        """
            insert a (sha1, songID, timeoffset) into MySQLdb
            key => sha1, value => (birdID, timeoffset)
        """
        try:
            args = (value[0], key, value[1])
            sql = MySQLDatabases.INSERT_FINGERPRINT % (value[0], key, value[1])

            with open('Logs/hashes.txt', 'a+') as hashes:
                hashes.write("%s\n" % sql)
                hashes.close()

            self.cursor.execute(MySQLDatabases.INSERT_FINGERPRINT, args)
            self.connection.autocommit(True)
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e',
                                   ("{insert()} Query Error: %d: %s SQL: %s" %
                                    (e.args[0], e.args[1], (MySQLDatabases.INSERT_FINGERPRINT, args))))

    def get_sounds(self):
        """
            return a list containing birdID & songnames
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_NON_FINGERPRINTED_SOUNDS)
            return self.cursor.fetchall()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_sounds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], self.SELECT_SOUNDS)))
            return None

    def get_matches(self, hashes):
        """
            return (bird_id, offset) tuples
               of sha1-> (None, sample_offset) vals
        """
        matches = []
        for h in hashes:
            sha1, val = h
            tups_lst = self.query_db(sha1)
            if tups_lst:
                for tup in tups_lst:
                    matches.append((tup[0], tup[1] - val[1]))
        return matches

    def query_db(self, key):
        """
            return all tuples associated with the hash

            if hash is None, return all entries
        """
        #select all if no key is given
        if key is None:
            sql = MySQLDatabases.SELECT_ALL
        else:
            sql = MySQLDatabases.SELECT

        matches = []
        try:
            self.cursor.execute(sql, (key,))

            #get all matches
            rows = self.cursor.fetchall()
            for row in rows:
                matches.append((row['birdID'], row['start_time']))
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{query_db()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))
        return matches

    def update_fingerprinted_songs(self, birdID):
        """
            update fingerprinted songs in db..to avoid re-fingerprinting
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.UPDATE_SONG_FINGERPRINTED, birdID)
            self.connection.commit()
            self.logging.write_log('databases', 'i', "finished fingerprinting -> birdId: %s " % str(birdID))
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{update_fingerprinted_songs()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], self.UPDATE_SONG_FINGERPRINTED)))

    def insert_stats(self, birdID, match_time, confidence, offset):
        """
            store match stats...later used for ML
        """
        sql = MySQLDatabases.INSERT_STATS % (birdID, match_time, confidence, offset)
        print sql

        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql)
            self.connection.commit()
            self.logging.write_log('databases', 'i', "new stats: confidence: %s" % str(confidence))
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_stats()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))

    def insert_tmp_sounds(self, birdID, soundType, wavFile, soundURL):
        """
            temporarily store sound details in db before downloading
            the sounds files
        """
        sql = None
        try:
            self.cursor = self.connection.cursor()
            sql = MySQLDatabases.INSERT_TMP_SOUNDS % (clean(str(birdID)), clean(wavFile), clean(soundType), soundURL)
            self.cursor.execute(sql)
            self.connection.commit()
            return True
        except mysql.Error, e:
            self.connection.rollback()
            self.logging.write_log('databases', 'e', ("{insert_tmp_sounds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], sql)))
            return False

    def get_no_tmp_sounds(self):
        """
            return no of tmp sounds in db
        """
        sql = "select * from tmp_sounds"
        self.cursor = self.connection.cursor()
        self.cursor.execute(sql)
        return int(self.cursor.rowcount)

    def get_tmp_sounds(self):
        """
            get sound details tmp. stored in MySQLdb
        """
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(MySQLDatabases.SELECT_TMP_SOUNDS)
            return self.cursor.fetchall()
        except mysql.Error, e:
            self.logging.write_log('databases', 'e', ("{get_tmp_sounds()} Query Error: %d: %s SQL: %s" %
                                                      (e.args[0], e.args[1], self.SELECT_TMP_SOUNDS)))
            return None
