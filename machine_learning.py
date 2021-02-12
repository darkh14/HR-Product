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

        self.threshold = kwargs.get('threshold') or 0
        self._filter = self._get_filter(kwargs.get('filter'))
        self._filter_rules = kwargs.get('filter_rules')

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

        self.db_connector.connect()
        if self.db_connector.is_connected:
            self._cv = self.db_connector.get_cv()

            if self._cv:

                count = 1

                for cv_line in self._cv.find():

                    add = variant == 'all' or variant == 'fitting' and self._cv_need_to_add(cv_line)
                    if add:
                        self._add_cv_line(cv_line)

                    count += 1

        self._fitting_cv.sort(key=lambda x: x['threshold'], reverse=True)

        if self.limit:
            cv_copy = self._fitting_cv.copy()
            self._fitting_cv.clear()
            counter = 1
            for cv_line in cv_copy:
                if counter <= self.limit:
                    self._fitting_cv.append(cv_line)
                else:
                    break
                counter += 1

        return self._fitting_cv

    def _cv_need_to_add(self, cv_line):

        result = True
        av_threshold = 0
        count = 0

        for filter_element in self._filter_rules:

            element_result = self._process_filter_element(cv_line, filter_element)

            if element_result == -1:
                result = False
                break
            if element_result != -2:
                av_threshold += element_result

            count += 1

        if result:
            result = self.threshold == -1 or (av_threshold/count if count else av_threshold) >= self.threshold

        cv_line['threshold'] = av_threshold/count

        return result

    def _process_filter_element(self, cv_row, filter_element):

        filter_field_par = filter_element.get('filter_field')

        field = cv_row.get(filter_element.get('field'))

        field_desc = self._comp_fields.get(filter_element.get('field'))

        result = 0

        if filter_element.get('type') == 'special':
            filter_field = self._filter.get(filter_field_par)
            result = self._process_special_parameter(field, filter_field, filter_element, cv_row)
        else:
            if field_desc:
                if field_desc['type'] == 'array':
                    filter_field = self._filter.get(filter_field_par)
                    result = self._process_array_parameter(field, filter_field, filter_element, field_desc)
                elif field_desc['type'] == 'dict':
                    result = self._process_dict_parameter(field, filter_field_par, filter_element)
                elif field_desc['type'] == 'array_dict':
                    result = self._process_array_parameter(field, filter_field_par, filter_element, field_desc)
            else:
                filter_field = self._filter.get(filter_field_par)
                result = self._process_simple_parameter(field, filter_field, filter_element)

        if filter_element.get('koef') and result != -1:
            result *= filter_element['koef']

        return result

    def _process_simple_parameter(self, field, filter_field, filter_element):

        filter_type, filter_comp = filter_element['type'], filter_element['comp']
        result = 0

        if filter_type == 'hard' or filter_type == 'soft':

            negative_result = -1 if filter_comp == 'hard' else 0

            if filter_comp == 'eq':
                result = 1 if field == filter_field else negative_result
            if filter_comp == 'eq_list':
                result = 1 if filter_field in field else negative_result
            if filter_comp == 'ne':
                result = 1 if field != filter_field else negative_result
            elif filter_comp == 'lt':
                result = 1 if field < filter_field else negative_result
            elif filter_comp == 'gt':
                result = 1 if field < filter_field else negative_result
            elif filter_comp == 'le':
                result = 1 if field <= filter_field else negative_result
            elif filter_comp == 'ge':
                result = 1 if field <= filter_field else negative_result

        elif filter_type == 'threshold' or filter_type == 'hard_threshold':

            koef = filter_element.get('koef') or 1

            if filter_comp == 'eq':
                result = 1 if field == filter_field else 0
            if filter_comp == 'eq_list':
                result = 1 if filter_field in field else 0
            if filter_comp == 'ne':
                result = 1 if field != filter_field else 0
            elif filter_comp == 'lt':
                result = 1 if field < filter_field else 0
            elif filter_comp == 'gt':
                result = 1 if field < filter_field else 0
            elif filter_comp == 'le':
                result = 1 if field <= filter_field else 0
            elif filter_comp == 'ge':
                result = 1 if field <= filter_field else 0
            elif filter_comp == 'sim':
                result = self._similarity(field, filter_field)
            elif filter_comp == 'sim_list':
                result = 0
                for el_field in field:
                    result += self._similarity(el_field, filter_field)
                if len(field):
                    result /= len(field)

            result *= koef

            if filter_type == 'hard_threshold':
                threshold = filter_element.get('threshold') or 0.5
                result = 1 if result >= threshold else -1

        return result

    def _process_array_parameter(self, field, filter_field, filter_element, field_desc):

        result_sum = 0
        count = 1

        for array_element in field:

            if field_desc['type'] == 'array_dict':
                element_result = self._process_dict_parameter(array_element, filter_field, filter_element)
            else:
                element_result = self._process_simple_parameter(array_element, filter_field, filter_element)
            if element_result == -1:
                if filter_element.get('type') == 'hard':
                    result_sum = -1
                    break
                else:
                    count += 1
            else:
                result_sum += element_result
                count += 1

        if filter_element.get('type') == 'soft':
            if result_sum == 0:
                result_sum = -1
            else:
                result_sum = 1
        elif result_sum != -1 :
            result_sum /= count

        return result_sum

    def _process_dict_parameter(self, field, filter_field, filter_element):
        result_sum = 0

        for filter_dict_el in filter_element.get('filter_field'):
            field_element = field.get(filter_dict_el.get('field'))
            element_result = self._process_simple_parameter(field_element,
                                                            self._filter.get(filter_dict_el.get('filter_field')),
                                                            filter_dict_el)
            if filter_dict_el['type'] == 'hard' and element_result == -1:
                result_sum = -1
                break
            else:
                result_sum += element_result

        if filter_element.get('type') == 'soft' and result_sum == 0:
            result_sum = -1

        if result_sum != -1:
            result_sum = 0 if not filter_element.get('filter_field') else (result_sum /
                                                                           len(filter_element.get('filter_field')))

        return result_sum

    def _process_special_parameter(self, field, filter_field, filter_element, cv_row):

        result = 0
        field_name = filter_element.get('filter_field')

        if field_name == 'texts':

            # about_me_list = cv_row['about_me'].lower().split(' ')
            el_result = 0

            for text in filter_field:

                # position
                position_result = 0
                res = self._similarity(cv_row['position'], text)
                if res > position_result:
                    position_result = res

                # about_me
                about_me_result = self._similarity(text, cv_row['about_me'])
                # about_me_count = 0
                #
                # text_list = text.lower().split(' ')
                # for text_el in text_list:
                #     about_me_count += about_me_list.count(text_el)
                #
                # about_me_result = 0 if about_me_count == 0 else about_me_count/(len(text_list)*len(about_me_list))

                if position_result < 0.7 and about_me_result < 0.2:
                    result = -1
                else:
                    el_result = position_result*0.7 + about_me_result*0.3

                if el_result > result:
                    result = el_result

        elif field_name == 'education':
            edu_com = {'Высшее образование': ['Высшее образование', 'Higher education', 'Вища освіта'],
                       'Адъюнктура': ['Адъюнктура'],
                       'Аспирантура': ['Аспирантура'],
                       'Высшее образование - бакалавриат': ['Высшее образование (Бакалавр)',
                                                            'Higher education (bachelor)'],
                       'Высшее образование - подготовка кадров высшей квалификации': [],
                       'Высшее образование - специалитет, магистратура': ['Высшее образование (Магистр)',
                                                                          'Высшее образование (Магистр)'],
                       'Докторантура': ['Высшее образование (Доктор наук)'],
                       'Дополнительное профессиональное образование': [],
                       'Интернатура': [],
                       'Начальное профессиональное образование': [],
                       'Неполное высшее образование': ['Неоконченное высшее образование', 'Incomplete higher education'],
                       'Ординатура': [],
                       'Основное общее образование': [],
                       'Послевузовское образование': [],
                       'Профессиональное обучение': [],
                       'Среднее (полное) общее образование': ['Среднее образование','Secondary education'],
                       'Среднее образование': ['Среднее образование', 'Secondary education'],
                       'Среднее профессиональное образование': ['Среднее специальное образование',
                                                                'Secondary special education']}

            edu_list = edu_com.get(filter_field)

            if edu_list:
                if cv_row['education_level'] in edu_list:
                    result = 1
                else:
                    result = -1 if filter_element.get('parameter') == 'hard' else 0
            else:
                result = -1 if filter_element.get('parameter') == 'hard' else 0

        elif field_name == 'age':
            age_from = self._filter.get('age_from')
            age_to = self._filter.get('age_to')

            age = int(cv_row['age'])
            # print(str(age) + '  ' + str(age_from) + ' - ' + str(age_to))
            if (not age_from or age >= age_from) and (not age_to or age <= age_to):
                result = 1
            else:
                result = -1 if filter_element.get('parameter') == 'hard' else 0

        elif field_name == 'salary':

            age_from = self._filter.get('salary_from')
            age_to = self._filter.get('salary_to')
            age = cv_row['salary']

            if (not age_from or age >= age_from) and (not age_to or age <= age_to):
                result = 1
            else:
                result = -1 if filter_element.get('parameter') == 'hard' else 0

        elif field_name == 'seniority':

            seniority_from = self._filter.get('salary_from')
            seniority_to = self._filter.get('salary_to')
            seniority = int(cv_row['seniority']['years'])

            if seniority_from == 0 and seniority_to == 0:
                result = 1 if seniority == 0 else 0
            elif seniority_from == 0 and seniority_to == 999:
                result = 1
            elif seniority_to == 999:
                result = 1 if seniority >= seniority_from else 0
            else:
                result = 1 if seniority_from <= seniority <= seniority_to else 0

            if filter_element.get('parameter') == 'hard' and result == 0:
                result = -1

        elif field_name == 'gender':
            f_gender = self._filter.get('gender')

            if f_gender:
                result = 1 if f_gender == cv_row['gender'] else 0
            else:
                result = 1

            if filter_element.get('parameter') == 'hard' and result == 0:
                result = -1

        elif field_name == 'work_schedule':
            schedule_list = cv_row['work_schedule'].replace(' ', '').split(',')
            f_schedule = self._filter.get('work_schedule')
            if f_schedule:
                if f_schedule in schedule_list:
                    result = 1
                else:
                    result = 0
            else:
                result = 1

            if filter_element.get('parameter') == 'hard' and result == 0:
                result = -1

        return result

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


def set_profiles(**kwargs):

    ml_processor = MLProcessor(**kwargs)
    is_set = ml_processor.set_profiles()

    return is_set, ml_processor.error