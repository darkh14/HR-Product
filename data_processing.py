from mongo_connection import MongoDBConnector
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
# from tensorflow.keras.utils import plot_model


class Processor:

    def __init__(self, **kwargs):
        self.cv = None
        self.vacancies = None
        self.prepared_cv = None
        self.labels = None
        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()
        self.fields = []
        self.error = ''

    def transform(self):
        self.cv = self.db_connector.read_cv()
        self.vacancies = self.db_connector.read_vacancies()
        self.labels = self.db_connector.read_labels()

        self.prepared_cv = pd.merge(self.cv, self.vacancies, on='key').reset_index()

        self.prepared_cv = self.prepared_cv.rename({'_id_x': '_id', '_id_y': '_id_vacancy', 'gender_x': 'gender', 'gender_y': 'gender_vacancy',
                    'position_x': 'position', 'position_y': 'position_vacancy', 'education_x': 'education',
                    'education_y': 'education_vacancy', 'work_schedule_x': 'work_schedule',
                    'work_schedule_y': 'work_schedule_vacancy'}, axis='columns')

        self.prepared_cv['is_this_vacancy'] = self.prepared_cv[['vacancy_id', 'vacancies']].apply(self._vacancy_compare, axis=1)
        self.prepared_cv['cv_id'] = self.prepared_cv['site_id']

        for field_name in self.fields:
            self.transform_field(field_name)

        self.db_connector.write_prepared_cv(self.prepared_cv)

    def fit(self):
        self.prepared_cv = self.db_connector.read_prepared_cv()

        scaler = MinMaxScaler()

        pd_data = self.prepared_cv[self.prepared_cv.columns[self.prepared_cv.columns != 'cv_id']]

        names = pd_data.columns
        data = scaler.fit_transform(pd_data)

        pd_data = pd.DataFrame(data, columns=names)
        self.prepared_cv[self.prepared_cv.columns[self.prepared_cv.columns != 'cv_id']] = pd_data

        X = self.prepared_cv[self.prepared_cv.columns[self.prepared_cv.columns != 'cv_id']].to_numpy()

        y = X[:, -1]
        X = X[:, :-1]

        model = self.load_model(True)

        model.fit(X, y, epochs=100, verbose=1, validation_split=0.2)  # batch_size=128,

        self.save_model(model)

    def predict(self, X):
        y_pred = None
        model = self.load_model()
        if model:
            model.predict(X)

        return y_pred

    @staticmethod
    def _vacancy_compare(X):
        v_id = X[0]
        v_list = X[1]
        return v_id in [vacancy['vacancy_id'] for vacancy in v_list]

    def transform_field(self, field_name):
        pass

    def load_model(self, create_if_nessesary=False):

        model = None
        if create_if_nessesary:
            model = Sequential()
            model.add(Dense(1024, activation="relu", input_shape=(147,), name='Dense_1'))
            model.add(Dense(100, activation="relu", name='Dense_2'))
            model.add(Dense(1, activation="sigmoid", name='Dense_3'))
            # model.summary()
            # plot_model(model, to_file='model.png')

            model.compile(optimizer='adam', loss='BinaryCrossentropy', metrics=['accuracy'])

        return model

    def save_model(self, model):
        pass


def transform(**kwargs):

    processor = Processor(**kwargs)
    processor.transform()

    return True, processor.error


def fit(**kwargs):

    processor = Processor(**kwargs)
    processor.fit()

    return True, processor.error


def predict(X, **kwargs):

    processor = Processor(**kwargs)
    processor.predict(X)

    return True, processor.error

