import pymongo

class MongoDBConnector:
    def __init__(self, host='', user='', password='', uri=''):
        if host:
            self.host = host
        else:
            self.host = 'mongo.nfp2b.ml'

        if user:
            self.user = user
        else:
            self.user = 'admin'  # 'hr-admin'

        if password:
            self.password = password
        else:
            self.password = '09hradminS'
        if uri:
            self.uri = uri
        else:
            # self.uri = "mongodb://%s:%s@%s/?authSource=admin" % (quote_plus(self.user), quote_plus(self.password), self.host)
            self.uri = "mongodb://%s:%s@%s/?authSource=admin" % (self.user, self.password, self.host)

        self.connection = None
        self.is_connected = False
        self.db = None
        self.is_db_chosen = False
        self.collection = None
        self.is_collection_chosen = False
        self.error = ''

    def connect(self, uri='', host='', user='', password=''):

        if uri != '':
            self.uri = uri

        if host != '':
            self.host = host

        if user != '':
            self.user = user

        if password != '':
            self.password = password

        if not self.is_connected:
            if self.uri == '':
                # self.uri = "mongodb://%s:%s@%s/?authSource=admin" % (quote_plus(user), quote_plus(password), host)
                self.uri = "mongodb://%s:%s@%s/?authSource=admin" % (user, password, host)

            self.connection = pymongo.MongoClient(self.uri)
            self.is_connected = True

        return True

    def get_connection(self):
        return self.connection

    def get_db_names(self):
        if self.is_connected:
            try:
                return self.connection.list_database_names()
            except pymongo.errors.ServerSelectionTimeoutError as MongoTimeoutError:
                self._add_error_text_('Connection error: ' + str(MongoTimeoutError))
                return None
        else:
            self._add_error_text_('Connection error')
            return None

    def get_hr(self):
        if not self.is_db_chosen:
            db_names = self.get_db_names()

            if not db_names:
                self._add_error_text_('Db names are not defined')
                return None
            if 'hr' in db_names:
                self.db = self.connection.hr
                self.is_db_chosen = True
                return self.db
            else:
                self._add_error_text_('"Hr" is not found in Db names')
                return None
        else:
            return self.db

    def get_collection_names(self):

        db = self.get_hr()

        if db:
            return db.list_collection_names()
        else:
            self._add_error_text_('"Hr" is not found in Db names')
            return None

    def get_cv(self):
        if not self.is_collection_chosen:
            collection_names = self.get_collection_names()

            if not collection_names:
                self._add_error_text_('Collection names are not defined')
                return None
            if 'cv' in collection_names:
                self.collection = self.db.get_collection('cv')
                self.is_collection_chosen = True
                return self.collection
            else:
                self._add_error_text_('"Cv" is not found in collection names')
                return None
        else:
            return self.collection

    def _add_error_text_(self, error_text):
        self.error = self.error + ('; ' if self.error else '') + error_text
