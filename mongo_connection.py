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
        self._collections = {}
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
            # self._add_error_text_('"Hr" is not found in Db names')
            return None

    def get_cv(self):
        return self._get_collection('cv')

    def write_cv_vacancy_line(self, cv_vacancy_line):
        return self.write_line('cv_vacancy_labels', cv_vacancy_line, ['cv_id', 'vacancy_id', 'manager', 'DB'])

    def write_cv_line(self, cv_line):
        return self.write_line('cv', cv_line, ['site_id'])

    def write_cv_vacancy_labels(self, cv_vacancy_labels):
        for label in cv_vacancy_labels:
            self.write_line('cv_vacancy_labels', label, ['cv_id', 'vacancy_id', 'manager', 'DB'])

    def write_vacancies(self, dataset):
        for line in dataset:
            self.write_line('vacancies', line, ['vacancy_id', 'DB'])

    def clear_cv_vacancy_labels(self):
        self._clear_collection('cv_vacancy_labels')

    def clear_vacancies(self):
        self._clear_collection('vacancies')

    def clear_cv_vacancies(self):
        self._clear_collection('vacancy')

    def _get_collection(self, collection_name):
        cur_collection = self._collections.get(collection_name)

        if not cur_collection:
            collection_names = self.get_collection_names()

            if not collection_names:
                self._add_error_text_('Collection names are not defined')
                return None
            if collection_name in collection_names:
                cur_collection = self.db.get_collection(collection_name)
                self._collections[collection_name] = cur_collection
                return cur_collection
            else:
                cur_collection = self.db.get_collection(collection_name)
                self._collections[collection_name] = cur_collection
                return cur_collection
        else:
            return cur_collection

    def _write_data(self, collection_name, data, multiline=False):
        collection = self._get_collection(collection_name)
        if collection:
            result = collection.insert_many(data) if multiline else collection.insert_one(data)
        else:
            result = None
        return result

    def write_line(self, collection_name, line, id_columns=[]):

        if not id_columns:
            return self._write_data(collection_name, line)

        id_filter = {}
        for id_column in id_columns:
            id_filter[id_column] = line[id_column]

        collection = self._get_collection(collection_name)
        if collection:
            id_filter = dict(filter(lambda item: item[0] in id_columns, line.items()))
            exists = collection.find_one(id_filter)
            if exists:
                result = collection.update_one(id_filter, {'$set': line})
            else:
                result = collection.insert_one(line)
        else:
            result = None
        return result

    def _clear_collection(self, collection_name):
        collection = self._get_collection(collection_name)
        if collection:
            collection.drop()

    def _add_error_text_(self, error_text):
        self.error = self.error + ('; ' if self.error else '') + error_text
