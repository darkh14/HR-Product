import pymongo
import settings
import datetime


class MongoDBConnector:
    def __init__(self, **kwargs):

        self.settings = kwargs.get('settings') or settings.SettingsController()
        self.host = kwargs.get('host') or 'mongo.nfp2b.ml'
        self.user = kwargs.get('user') or 'admin'
        self.password = kwargs.get('password') or '09hradminS'

        self.uri = (kwargs.get('uri') or
                    self.settings.get_parameter('mongo_uri') or
                    "mongodb://%s:%s@%s/?authSource=admin" % (self.user, self.password, self.host))

        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.db = None
        self.is_db_chosen = False
        self._collections = {}
        self.error = ''

    def connect(self, **kwargs):

        host = kwargs.get('host') or self.host
        user = kwargs.get('user') or self.user
        password = kwargs.get('password') or self.password
        from_host = bool(host and user and password)
        c_settings = kwargs.get('settings') or self.settings
        from_settings = kwargs.get('from_settings')
        if from_settings is None:
            from_settings = False
        reconnect = kwargs.get('reconnect')
        if reconnect is None:
            reconnect = False

        uri = (kwargs.get('uri')
               or self.uri
               or (from_host and "mongodb://%s:%s@%s/?authSource=admin" % (user, password, host))
               or (from_settings and c_settings.get_parameter('mongo_uri')))

        if reconnect or not self.is_connected:
            self.connection = pymongo.MongoClient(uri)
            self.is_connected = True
            self.get_hr()

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

        if not self.is_connected:
            self.connect()

        if not self.is_db_chosen:
            self.db = self.connection['hr']
            self.is_db_chosen = True
            return self.db
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
        cv = self._get_collection('cv')
        return cv

    def write_cv_vacancy_line(self, cv_vacancy_line):
        return self.write_line('cv_vacancy_labels', cv_vacancy_line, ['cv_id', 'vacancy_id', 'manager', 'DB'])

    def write_cv_line(self, cv_line):
        vacancies = []
        prev_line = self.read_line('cv', {'site_id': cv_line['site_id']})

        if prev_line and prev_line.get('vacancies'):
            vacancies = prev_line.get('vacancies')

        if cv_line['vacancy_id'] and cv_line['db']:

            need_to_add = True
            for prev_vacancy in vacancies:
                if (prev_vacancy.get('vacancy_id') == cv_line.get('vacancy_id')
                   and prev_vacancy['db'] == cv_line['db']):
                    need_to_add = False
                    break

            if need_to_add:
                vacancy_dict = {'vacancy_id': cv_line['vacancy_id'], 'db': cv_line['db'],
                                'profile_id': cv_line.get('profile_id') or ''}
                vacancies.append(vacancy_dict)

            cv_line.pop('profile_id', '')
            cv_line.pop('vacancy_id')
            cv_line.pop('db')

        cv_line['vacancies'] = vacancies

        return self.write_line('cv', cv_line, ['site_id'])

    def write_cv_vacancy_labels(self, cv_vacancy_labels):
        for label in cv_vacancy_labels:
            self.write_line('cv_vacancy_labels', label, ['cv_id', 'vacancy_id', 'profile_id', 'manager', 'db'])

    def write_vacancies(self, dataset):
        for line in dataset:
            self.write_line('vacancies', line, ['vacancy_id', 'db'])

    def write_profiles(self, dataset):
        for line in dataset:
            self.write_line('profiles', line, ['profile_id', 'db'])

    def clear_cv_vacancy_labels(self):
        self.clear_collection('cv_vacancy_labels')

    def clear_vacancies(self):
        self.clear_collection('vacancies')

    def clear_profiles(self):
        self.clear_collection('profiles')

    def delete_jobs(self, filter):
        self.delete_lines('jobs', filter)

    def delete_lines(self, collection_name, filter):

        if not self.is_connected:
            self.connect()

        collection = self._get_collection(collection_name)
        collection.delete_many(filter)

    def clear_cv_vacancies(self):
        self.clear_collection('vacancy')

    def _get_collection(self, collection_name):

        if not self.is_connected:
            self.connect()

        cur_collection = self._collections.get(collection_name)

        if not cur_collection:
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

    def write_line(self, collection_name, line, id_columns=None):

        if not self.is_connected:
            self.connect()

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

    def write_job(self, line, id_columns):
        str_today, timestamp_today = self._get_today()
        line['date'] = str_today
        line['timestamp'] = timestamp_today
        line.setdefault('info', '')

        self.write_line('jobs', line, id_columns)

    def read_line(self, collection_name, id_filter):
        if not self.is_connected:
            self.connect()
        collection = self._get_collection(collection_name)
        return collection.find_one(id_filter)

    def read_collection(self, collection_name):
        if not self.is_connected:
            self.connect()
        collection = self._get_collection(collection_name)

        result = []
        for coll_line in collection.find():
            coll_line.pop('_id')
            result.append(coll_line)

        return result

    def read_job(self, id_filter):
        result = None
        if id_filter.get('job'):
            if id_filter.get('job_id'):
                result = self.read_line('jobs', id_filter)
            else:
                if not self.is_connected:
                    self.connect()
                collection = self._get_collection('jobs')
                result = collection.find_one(id_filter, sort=[("timestamp", pymongo.DESCENDING)])

        return result

    def read_jobs(self, job_filter, limit=0):

        lines = []
        if not self.is_connected:
            self.connect()

        collection = self._get_collection('jobs')

        finder = collection.find(job_filter, sort=[("timestamp", pymongo.DESCENDING)])

        if limit:
            finder = finder.limit(limit)

        for line in finder:
            line.pop('_id')
            lines.append(line)

        return lines

    def clear_collection(self, collection_name):
        collection = self._get_collection(collection_name)
        if collection:
            collection.drop()

    def _add_error_text_(self, error_text):
        self.error = self.error + ('; ' if self.error else '') + error_text

    @staticmethod
    def _get_today():
        today = datetime.datetime.today()
        return today.strftime("%Y.%m.%d %H:%M:%S"), today.timestamp()

