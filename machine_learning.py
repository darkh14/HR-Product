# -*- coding: utf-8 -*-

from mongo_connection import MongoDBConnector
import difflib
import re
from filter import Filter
import time


class MLProcessor:

    def __init__(self, **kwargs):

        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()

        self._cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                           'specialization', 'employment', 'work_schedule', 'seniority', 'experience',
                           'skills', 'education_level', 'education', 'resume_link', 'site_id', 'site_url', 'threshold',
                           'site']
        self._comp_fields = {'specialization': {'type': 'array'},
                             'seniority': {'type': 'dict', 'fields': ['years', 'months']},
                             'experience': {'type': 'array_dict',
                                            'fields': ['start', 'end', 'timeinterval', 'company', 'position',
                                                       'description']},
                             'skills': {'type': 'array'},
                             'education': {'type': 'array_dict',
                                           'fields': ['final', 'name', 'organization']},
                             'vacancies': {'type': 'array_dict',
                                           'fields': ['vacancy_id', 'profile_id', 'db']}
                             }
        self._vacancy_fields = ['vacancy_id', 'db', 'name', 'position', 'duties',
                                'unit', 'requirements', 'conditions', 'age_from', 'age_to', 'education',
                                'workplace', 'gender', 'estimated_revenue', 'work_experience_from',
                                'work_experience_to', 'work_schedule', 'employment_type', 'contract_type',
                                'organization', 'profile']

        self._profile_fields = ['profile_id', 'db', 'name', 'position', 'duties',
                                'unit', 'requirements', 'conditions', 'age_from', 'age_to', 'education',
                                'workplace', 'gender', 'estimated_revenue', 'work_experience_from',
                                'work_experience_to', 'work_schedule', 'employment_type', 'contract_type',
                                'organization']

        self._cv_vacancy_labels_fields = ['cv_id', 'vacancy_id', 'profile_id', 'manager', 'db', 'fits', 'active']

        self.filter_processor = Filter(**kwargs)

        self.threshold = kwargs.get('threshold') or 0
        self._filter_names = self.filter_processor.get_filter_names()
        self._filter = self._get_filter(kwargs.get('filter'))

        self._clear_vacancies = bool(kwargs.get('clear_vacancies'))
        self._vacancies = self._get_vacancies_from_parameters(kwargs.get('vacancies') or [])

        self._clear_profiles = bool(kwargs.get('clear_profiles'))
        self._profiles = self._get_profiles_from_parameters(kwargs.get('profiles') or [])

        self._clear_labels = bool(kwargs.get('clear_labels'))
        self._cv_vacancy_labels = self._get_cv_vacancy_labels_from_parameters(kwargs.get('cv_vacancy_labels') or [])

        self._cv = []
        self._fitting_cv = []
        self.error = ''
        self.kwargs = kwargs
        self.limit = kwargs.get('limit') or 0

    def find_fitting_cvs(self, **kwargs):
        simple = bool(kwargs.get('simple'))

        if simple:
            cvs = self._simple_find_fitting_cvs(**kwargs)
        else:
            cvs = []  # neural network
        return cvs

    def get_all_cvs(self):
        return self._fill_cvs(variant='all')

    def set_cv_vacancy_labels(self, **kwargs):

        cv_vacancy_labels = kwargs.get('cv_vacancy_labels') or self._cv_vacancy_labels

        clear_labels = kwargs.get('clear_labels') if kwargs.get('clear_labels') is not None else self._clear_labels

        is_set = False
        self.db_connector.connect(**kwargs)
        if self.db_connector.is_connected:

            if clear_labels:
                self.db_connector.clear_cv_vacancy_labels()

            self.db_connector.write_cv_vacancy_labels(cv_vacancy_labels)
            is_set = True
        return is_set

    def set_vacancies(self, **kwargs):

        vacancies = kwargs.get('vacancies') or self._vacancies
        clear_vacancies = kwargs.get('clear_vacancies') if (kwargs.get('clear_vacancies')
                                                            is not None) else self._clear_vacancies

        is_set = False

        self.db_connector.connect(**kwargs)
        if self.db_connector.is_connected:

            if clear_vacancies:
                self.db_connector.clear_vacancies()

            self.db_connector.write_vacancies(vacancies)
            is_set = True
        return is_set

    def set_profiles(self, **kwargs):

        profiles = kwargs.get('profiles') or self._profiles
        clear_profiles = kwargs.get('clear_profiles') if (kwargs.get('clear_profiles')
                                                           is not None) else self._clear_profiles

        is_set = False

        self.db_connector.connect(**kwargs)
        if self.db_connector.is_connected:

            if clear_profiles:
                self.db_connector.clear_profiles()

            self.db_connector.write_profiles(profiles)
            is_set = True
        return is_set

    def _get_filter(self, cv_filter):

        if cv_filter:
            for filter_name in self._filter_names:
                cv_filter.setdefault(filter_name, '')
        else:
            cv_filter = {}

        return cv_filter

    def _get_vacancies_from_parameters(self, vacancies=None):
        if not hasattr(self, '_vacancies'):
            self._vacancies = []
        vacancies = vacancies or self._vacancies or []
        temp_lines = []
        for line in vacancies:
            if line.get('vacancy_id'):
                for field in self._vacancy_fields:
                    line.setdefault(field, '')
                temp_lines.append(line)

        return temp_lines

    def _get_profiles_from_parameters(self, profiles=None):
        if not hasattr(self, '_profiles'):
            self._profiles = []
        profiles = profiles or self._profiles or []
        temp_lines = []
        for line in profiles:
            if line.get('profile_id'):
                for field in self._profile_fields:
                    line.setdefault(field, '')
                temp_lines.append(line)

        return temp_lines

    def _get_cv_vacancy_labels_from_parameters(self, labels):
        if not hasattr(self, '_cv_vacancy_labels'):
            self._cv_vacancy_labels = []
        labels = labels or self._cv_vacancy_labels or []
        temp_lines = []
        for line in labels:
            if line.get('cv_id') and line.get('vacancy_id') and line.get('profile_id'):
                for field in self._cv_vacancy_labels_fields:
                    line.setdefault(field, '')
                temp_lines.append(line)

        return temp_lines

    def _simple_find_fitting_cvs(self, **kwargs):
        return self._fill_cvs(variant='fitting', **kwargs)

    def _fill_cvs(self, variant='all', **kwargs):

        self._cv = self.db_connector.get_cv()

        start = time.time()

        if self._cv:

            count = 1

            mongo_filter = self.create_mongo_filter()

            cursor = self._cv.find(mongo_filter)
            if self.limit:
                cursor = cursor.limit(self.limit)

            for cv_line in cursor:
                if not self._check_text(cv_line):
                    continue

                cv_line.pop('_id')
                cv_line['threshold'] = self._get_threshold(cv_line)
                # print(str(count) + ' -- ' + cv_line['position'] + ' -- ' + str(cv_line['threshold']))
                self._fitting_cv.append(cv_line)

                count += 1

        self._fitting_cv.sort(key=lambda x: x['threshold'], reverse=True)

        count = 1
        for cv_line in self._fitting_cv:
            print(str(count) + ' -- ' + cv_line['position'] + ' -- ' + str(cv_line['threshold']))
            count += 1

        end = time.time()
        print('Время выполнения: ' + str(end - start))

        return self._fitting_cv

    def _check_text(self, cv_line):

        result = False
        fields = self.filter_processor.get_filter_value('text', 'settings', 'cv', True)
        rr = 0
        texts = self._filter['text'].get('value')

        words = []
        for text in texts:
            text_list = text.strip().split(' ')
            for text_word in text_list:
                if text_word not in words:
                    words.append(text_word.lower())

        for field in fields:
            for word in words:
                cv_text = cv_line[field].lower()
                result = cv_text.find(word) != -1

                if result:
                    break
            if result:
                break

        return result

    def create_mongo_filter(self):
        mongo_filter = {}
        filter_names = self._filter_names
        filter_list = []
        for filter_name in filter_names:
            if filter_name == 'text':
                continue
            filter_element = self._get_field_mongo_filter(filter_name)
            if filter_element:
                filter_list.append(filter_element)
        if len(filter_list) == 1:
            mongo_filter = filter_list[0]
        elif len(filter_list) > 1:
            mongo_filter = {'$and': filter_list}

        return mongo_filter

    def _get_field_mongo_filter(self, filter_name):
        mongo_filter = {}
        if filter_name == 'text':
            mongo_filter = self._create_text_mongo_filter()
        else:
            input_filter = self._filter.get(filter_name)
            if input_filter and input_filter.get('use'):
                value = self.filter_processor.get_filter_value(input_filter.get('value'), filter_name, 'cv', True)

                field_name = self.filter_processor.get_filter_value(filter_name, 'settings', 'cv')

                if field_name and value:
                    is_range = isinstance(value[0], dict) and len(value[0]) == 2
                    if is_range:
                        filter_gte = {field_name: {'$gte': value[0][filter_name + '_from']}}
                        filter_lte = {field_name: {'$lte': value[0][filter_name + '_to']}}
                        mongo_filter = {'$and': [filter_gte, filter_lte]}
                    elif len(value) == 1:
                        mongo_filter = {field_name: value[0]}
                    else:
                        mongo_filter = {field_name: {'$in': value}}

        return mongo_filter

    def _create_text_mongo_filter(self):
        mongo_filter = {}

        input_text_filter = self._filter.get('text')
        if input_text_filter and input_text_filter.get('use'):
            texts = input_text_filter.get('value')
            if texts:
                words = []
                for text in texts:
                    text_list = text.strip().split(' ')
                    for text_word in text_list:
                        if text_word not in words:
                            words.append(text_word)

                reg_exp_string = ''
                first = True
                for word in words:
                    reg_exp_string += ('' if first else '|') + word
                    first = False

                reg_exp_string = '.*' + reg_exp_string + '.*'
                rgx = re.compile(reg_exp_string, re.IGNORECASE)
                mongo_filter = {'$or': [{'position': {'$regex': rgx}}, {'about_me': {'$regex': rgx}}]}
                # mongo_filter = {'about_me': {'$regex': rgx}}

        return mongo_filter

    def _get_threshold(self, cv_line):
        threshold = 0
        for filter_name in self._filter_names:

            field_threshold = self._get_field_threshold(filter_name, cv_line)
            if field_threshold != -1:
                threshold += field_threshold

        self.first = False
        return threshold

    def _get_field_threshold(self, filter_name, cv_line):

        threshold = 0

        if filter_name == 'text':

            if self._filter.get('text'):

                fields = self.filter_processor.get_filter_value('text', 'settings', 'cv', True)

                count = 1
                for field in fields:
                    c_threshold = 0
                    c_text = ''
                    for text in self._filter['text'].get('value'):
                        c_text += ' ' + text

                    c_threshold += self._similarity(cv_line[field], c_text)

                    # c_threshold /= len(self._filter['text'])
                    if count == 1:
                        c_threshold *= 0.8
                    elif count == 2:
                        c_threshold *= 0.2
                    else:
                        c_threshold *= 0.1

                    threshold += c_threshold

                    count += 1
            else:
                threshold = -1
        else:
            input_filter = self._filter.get(filter_name)
            if input_filter and not input_filter.get('use'):
                value = self.filter_processor.get_filter_value(input_filter.get('value'), filter_name, 'cv', True)

                field_name = self.filter_processor.get_filter_value(filter_name, 'settings', 'cv')

                if field_name and value:
                    is_range = isinstance(value[0], dict) and len(value[0]) == 2
                    if is_range:

                        threshold = 0.01 if (value[0][filter_name + '_from'] <= cv_line[field_name]
                                             <= value[0][filter_name + '_to']) else 0
                    else:
                        threshold = 0.01 if value[0] == cv_line[field_name] else 0

        return threshold

    @staticmethod
    def _similarity(s1, s2):
        normalized1 = s1.lower()
        normalized2 = s2.lower()

        words1 = normalized1.strip().split(' ')
        words2 = normalized2.strip().split(' ')

        count = 0

        used_words = []
        for word1 in words1:
            for word2 in words2:
                if word1 == word2 and word1 not in used_words:
                    used_words.append(word1)
                    count += 1
        # matcher = difflib.SequenceMatcher(None, normalized1, normalized2)
        return count/max(len(words1), len(words2)) if words1 and words2 else 0
        # return matcher.ratio()

    @staticmethod
    def _get_id_from_link(link):
        q_list = link.split('?')
        if len(q_list) == 0:
            return ''
        s_list = q_list[0].split('/')
        if len(s_list) == 0:
            return ''

        return s_list[-1]


def find_fitting_ids(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    ids = ml_processor.find_fitting_cvs(simple=True)

    return ids, ml_processor.error


def get_all_ids(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    ids = ml_processor.get_all_cvs()

    return ids, ml_processor.error


def set_cv_vacancy_labels(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    is_set = ml_processor.set_cv_vacancy_labels()

    return is_set, ml_processor.error


def set_vacancies(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    is_set = ml_processor.set_vacancies()

    return is_set, ml_processor.error


def set_profiles(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    is_set = ml_processor.set_profiles()

    return is_set, ml_processor.error