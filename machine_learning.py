# -*- coding: utf-8 -*-

from mongo_connection import MongoDBConnector
import difflib


class MLProcessor:

    def __init__(self, **kwargs):

        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()

        self._filter_names = ['age_begin', 'age_end', 'gender', 'work_experience_begin',
                              'work_experience_end', 'education', 'requirements', 'duties']
        self._cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                           'specialization', 'employment', 'work_schedule', 'seniority', 'experience',
                           'skills', 'education_level', 'education', 'resume_link', 'site_id', 'site_url', 'threshold']
        self._comp_fields = {'specialization': {'type': 'array'},
                             'seniority': {'type': 'dict', 'fields': ['years', 'months']},
                             'experience': {'type': 'array_dict',
                                            'fields': ['start', 'end', 'timeinterval', 'company', 'position',
                                                       'description']},
                             'skills': {'type': 'array'},
                             'education': {'type': 'array_dict',
                                           'fields': ['final', 'name', 'organization']}}
        self._vacancy_fields = ['vacancy_id', 'DB', 'code', 'name', 'creation_date', 'position', 'duties',
                                'searching_parameters', 'unit', 'accordance', 'status', 'status', 'requirements',
                                'conditions', 'age_from', 'age_to', 'education', 'workplace', 'gender',
                                'estimated_revenue', 'profile', 'work_experience_from', 'work_experience_to',
                                'work_schedule', 'employment_type', 'contract_type', 'organization', 'grade']
        self._cv_vacancy_labels_fields = ['cv_id', 'vacancy_id', 'manager', 'db', 'fits', 'active']

        self.threshold = kwargs.get('threshold') or 0.565
        self._filter = self._get_filter(kwargs.get('filter'))

        self._clear_vacancies = bool(kwargs.get('clear_vacancies'))
        self._vacancies = self._get_vacancies_from_parameters(kwargs.get('vacancies') or [])

        self._clear_labels = bool(kwargs.get('clear_labels'))
        self._cv_vacancy_labels = self._get_cv_vacancy_labels_from_parameters(kwargs.get('cv_vacancy_labels') or [])

        self._cv = []
        self._fitting_cv = []
        self.error = ''
        self.kwargs = kwargs

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
        clear_vacancies = kwargs.get('clear_vacancies') if kwargs.get('clear_vacancies') is not None else self._clear_vacancies

        is_set = False

        self.db_connector.connect(**kwargs)
        if self.db_connector.is_connected:

            if clear_vacancies:
                self.db_connector.clear_vacancies()

            self.db_connector.write_vacancies(vacancies)
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

    def _get_cv_vacancy_labels_from_parameters(self, labels):
        if not hasattr(self, '_cv_vacancy_labels'):
            self._cv_vacancy_labels = []
        labels = labels or self._cv_vacancy_labels or []
        temp_lines = []
        for line in labels:
            if line.get('cv_id') and line.get('vacancy_id'):
                for field in self._cv_vacancy_labels_fields:
                    line.setdefault(field, '')
                temp_lines.append(line)

        return temp_lines

    def _simple_find_fitting_cvs(self, **kwargs):
        return self._fill_cvs(variant='fitting', **kwargs)

    def _fill_cvs(self, variant='all', **kwargs):

        self.db_connector.connect()
        if self.db_connector.is_connected:
            self._cv = self.db_connector.get_cv()

            if self._cv:

                for cv_line in self._cv.find():

                    cv_line['threshold'] = self._get_av_threshold(cv_line)
                    add = variant == 'all' or variant == 'fitting' and self._check_cv_row(cv_line)
                    if add:
                        self._add_cv_line(cv_line)

        self._fitting_cv.sort(key=lambda x: x['threshold'], reverse=True)

        return self._fitting_cv

    def _check_cv_row(self, cv_row):
        return cv_row['threshold'] >= self.threshold

    def _get_av_threshold(self, cv_row):
        threshold_list = []
        age_processed = False
        work_experience_processed = False
        for par_name in self._filter_names:

            if (par_name == 'age_begin' or par_name == 'age_end') and not age_processed:
                cur_threshold = self._process_range_parameter(cv_row.get('age'),
                                                              self._filter.get('age_begin'),
                                                              self._filter.get('age_end'),
                                                              True)
                threshold_list.append(cur_threshold)
                age_processed = True
            elif par_name == 'gender':
                gender_dict = {'Мужчина': 'm', 'Женщина': 'f'}
                cur_threshold = self._process_enum_parameter(cv_row.get('gender'),
                                                             self._filter.get(par_name),
                                                             gender_dict, True)
                threshold_list.append(cur_threshold)
            elif (par_name == 'work_experience_begin' or
                  par_name == 'work_experience_end') and not work_experience_processed:
                pass  # нет полей в таблице резюме для обработки данного параметра
            elif par_name == 'education':
                pass  # нет полей в таблице резюме для обработки данного параметр
            elif par_name == 'requirements':
                cur_threshold = 0
                cur_threshold += self._process_string_parameter(cv_row.get('position'), self._filter.get(par_name))
                cur_threshold += self._process_string_parameter(cv_row.get('category'), self._filter.get(par_name))
                cur_threshold += self._process_list_parameter(cv_row.get('specialization'), self._filter.get(par_name))
                cur_threshold /= 3
                threshold_list.append(cur_threshold)

            elif par_name == 'duties':
                cur_threshold = 0
                cur_threshold += self._process_string_parameter(cv_row.get('position'), self._filter.get(par_name))
                cur_threshold += self._process_string_parameter(cv_row.get('about_me'), self._filter.get(par_name))
                cur_threshold += self._process_list_parameter(cv_row.get('specialization'), self._filter.get(par_name))
                cur_threshold /= 3
                threshold_list.append(cur_threshold)

        av_threshold = sum(threshold_list)/len(threshold_list) if len(threshold_list) > 0 else 0

        return av_threshold

    def _add_cv_line(self, cv_line):
        cur_line = {}
        for name in self._cv_fields:
            comp_field = self._comp_fields.get(name)
            if name == '_id':
                cur_line[name] = str(cv_line.get(name))
            elif comp_field:
                cv_field = cv_line.get(name)
                # if cv_field:
                if comp_field['type'] == 'array':
                    cur_line[name] = []
                    if cv_field:
                        for element in cv_field:
                            cur_line[name].append(element)
                elif comp_field['type'] == 'dict':
                    cur_line[name] = {}
                    for comp_name in comp_field['fields']:
                        cur_line[name][comp_name] = cv_field.get(comp_name) if cv_field else ''
                elif comp_field['type'] == 'array_dict':
                    cur_line[name] = []
                    if cv_field:
                        for element in cv_field:
                            cur_line_row_element = {}
                            for comp_name in comp_field['fields']:
                                cur_line_row_element[comp_name] = element.get(comp_name)
                            cur_line[name].append(cur_line_row_element)
            else:
                cur_line[name] = cv_line.get(name)
        cur_line['site_id'] = self._get_id_from_link(cur_line['resume_link'])
        self._fitting_cv.append(cur_line)

    @staticmethod
    def _process_range_parameter(value, par_begin, par_end, endnul_as_inf=False):

        if not value:
            value = 0

        if type(value) is str:
            value = float(value)

        if not value:
            value = 0
        if not par_begin:
            par_begin = 0
        if not par_end:
            par_end = 0

        if par_begin == 0 and par_end == 0:
            if endnul_as_inf:
                result = 1
            elif value == 0:
                result = 1
            else:
                result = 0
        elif par_end == 0:
            if endnul_as_inf:
                result = 1
            else:
                result = 0
        elif par_begin <= value <= par_end:
            result = 1
        else:
            result = 0

        return result

    def _process_enum_parameter(self, value, parameter, comp_dict=None, none_as_true=False, precise=True):
        if not value:
            value = ''
        if not parameter:
            parameter = ''

        value = value.strip()

        if none_as_true and not value:
            result = 1
        else:
            if not comp_dict:
                if precise:
                    result = 1 if value == parameter else 0
                else:
                    result = self._similarity(value, parameter)
            else:
                cur_parameter = comp_dict.get(parameter)
                if cur_parameter:
                    if precise:
                        result = 1 if value == cur_parameter else 0
                    else:
                        result = self._similarity(value, cur_parameter)
                elif none_as_true:
                    result = 1
                else:
                    result = 0

        return result

    def _process_string_parameter(self, value, parameter, precise=False):

        if not value:
            value = ''
        if not parameter:
            parameter = ''

        if precise:
            result = 1 if value == parameter else 0
        else:
            result = self._similarity(value, parameter)

        return result

    def _process_list_parameter(self, value, parameter, precise=False):

        result = 0
        for cur_value in value:
            if precise:
                result += 1 if cur_value == parameter else 0
            else:
                result += self._similarity(cur_value, parameter)

        result /= len(value)
        return result

    @staticmethod
    def _similarity(s1, s2):
        normalized1 = s1.lower()
        normalized2 = s2.lower()
        matcher = difflib.SequenceMatcher(None, normalized1, normalized2)
        return matcher.ratio()

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
