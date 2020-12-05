from mongo_connection import MongoDBConnector
import difflib


class MLProcessor(MongoDBConnector):

    def __init__(self, uri='', parameters=None):
        super().__init__(uri=uri)
        self._cv = []
        self._fitting_cv = []
        self.error = ''
        self._cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                           'specialization', 'еmployment', 'work_schedule', 'seniority', 'experience',
                           'skills', 'education_level', 'education', 'resume_link', 'site_id']
        self._comp_fields = {'specialization': {'type': 'array'},
                              'seniority': {'type': 'dict', 'fields': ['years', 'months']},
                              'experience': {'type': 'array_dict',
                                    'fields': ['start', 'end', 'timeinterval', 'company', 'position', 'description']},
                              'skills': {'type': 'array'},
                              'education': {'type': 'array_dict',
                                    'fields': ['final', 'name', 'organization']}}
        self._cv_vacancy_labels_fields = ['cv_id', 'vacancy_id', 'fits', 'active']
        self._filter_parameters_names = ['AgeBegin', 'AgeEnd', 'Gender', 'WorkExperienceBegin',
                                         'WorkExperienceEnd', 'Education', 'Requirements', 'Duties']
        self._filter_is_set = False
        self._cv_vacancy_parameters_is_set = False
        self._filter_parameters = dict.fromkeys(self._filter_parameters_names, '')
        self.threshold = 0.565
        self._cv_vacancy_labels = []
        self.clear_labels = False

        if parameters:
            self._set_filter(parameters)
            if parameters.get('threshold'):
                self.threshold = parameters.get('threshold')
            self._set_cv_vacancy_parameters(parameters)

    def find_fitting_cvs(self, parameters=None, simple=False):

        if parameters:
            self._set_filter(parameters)

        if simple:
            cvs = self._simple_find_fitting_cvs(parameters)
        else:
            cvs = []  # neural network
        return cvs

    def get_all_cvs(self):

        return self._fill_cvs(variant='all')

    def set_cv_vacancy_labels(self, parameters=None, mongo_connection_string=''):
        is_set = False
        if parameters:
            self._set_cv_vacancy_parameters(parameters)
        self.connect(uri=mongo_connection_string)
        if self.is_connected:

            if self.clear_labels:
                self.clear_cv_vacancy_labels()

            self.write_cv_vacancy_labels(self._cv_vacancy_labels)
            is_set = True
        return is_set

    def _set_filter(self, filter_parameters):
        if not self._filter_is_set:
            for name in self._filter_parameters_names:
                par = filter_parameters.get(name)
                if par:
                    self._filter_parameters[name] = par
        self._filter_is_set = True

    def _set_cv_vacancy_parameters(self, parameters):

        if not self._cv_vacancy_parameters_is_set:
            self._cv_vacancy_labels = []
            labels = parameters.get('cv_vacancy_labels')

            self.clear_labels = parameters.get('clear_labels')

            if labels:
                for label in labels:

                    if label:
                        label_line = label.copy()

                        for label_name in self._cv_vacancy_labels_fields:
                            label_line.setdefault(label_name)

                        if label_line.get('cv_id') and label_line.get('vacancy_id'):
                            self._cv_vacancy_labels.append(label_line)

                self._cv_vacancy_parameters_is_set = True

    def _simple_find_fitting_cvs(self, parameters, mongo_connection_string=''):
        return self._fill_cvs(variant='fitting', parameters=parameters, mongo_connection_string=mongo_connection_string)

    def _fill_cvs(self, parameters=None, variant='all', mongo_connection_string=''):
        if variant == 'fitting':
            if parameters:
                self._set_filter(parameters)
        self.connect(uri=mongo_connection_string)
        if self.is_connected:
            self._cv = self.get_cv()

            for cv_line in self._cv.find():
                add = variant == 'all' or variant == 'fitting' and self._check_cv_row(cv_line)
                if add:
                    self._add_cv_line(cv_line)

        return self._fitting_cv

    def _check_cv_row(self, cv_row):
        threshold_list = []
        age_processed = False
        work_experience_processed = False
        for par_name in self._filter_parameters_names:

            if (par_name == 'AgeBegin' or par_name == 'AgeEnd') and not age_processed:
                cur_threshold = self._process_range_parameter(cv_row.get('age'),
                                                              self._filter_parameters.get('AgeBegin'),
                                                              self._filter_parameters.get('AgeEnd'),
                                                              True)
                threshold_list.append(cur_threshold)
                age_processed = True
            elif par_name == 'Gender':
                gender_dict = {'Мужчина': 'm', 'Женщина': 'f'}
                cur_threshold = self._process_enum_parameter(cv_row.get('gender'),
                                                             self._filter_parameters.get(par_name),
                                                             gender_dict, True)
                threshold_list.append(cur_threshold)
            elif (par_name == 'WorkExperienceBegin' or
                  par_name == 'WorkExperienceEnd') and not work_experience_processed:
                pass  # нет полей в таблице резюме для обработки данного параметра
            elif par_name == 'Education':
                pass  # нет полей в таблице резюме для обработки данного параметр
            elif par_name == 'Requirements':
                cur_threshold = 0
                cur_threshold += self._process_string_parameter(cv_row.get('position'), self._filter_parameters.get(par_name))
                cur_threshold += self._process_string_parameter(cv_row.get('category'), self._filter_parameters.get(par_name))
                cur_threshold += self._process_list_parameter(cv_row.get('specialization'), self._filter_parameters.get(par_name))
                cur_threshold /= 3
                threshold_list.append(cur_threshold)

            elif par_name == 'Duties':
                cur_threshold = 0
                cur_threshold += self._process_string_parameter(cv_row.get('position'), self._filter_parameters.get(par_name))
                cur_threshold += self._process_string_parameter(cv_row.get('about_me'), self._filter_parameters.get(par_name))
                cur_threshold += self._process_list_parameter(cv_row.get('specialization'), self._filter_parameters.get(par_name))
                cur_threshold /= 3
                threshold_list.append(cur_threshold)

        av_threshold = sum(threshold_list)/len(threshold_list) if len(threshold_list) > 0 else 0
        return av_threshold >= self.threshold

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
        cur_line['site_id'] = self.get_id_from_link(cur_line['resume_link'])
        self._fitting_cv.append(cur_line)

    @staticmethod
    def _process_range_parameter(value, par_begin, par_end, endnul_as_inf=False):
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
    def get_id_from_link(link):
        q_list = link.split('?')
        if len(q_list) == 0:
            return ''
        s_list = q_list[0].split('/')
        if len(s_list) == 0:
            return ''

        return s_list[-1]


def find_fitting_ids(parameters, mongo_connection_string):

    ml_processor = MLProcessor(uri=mongo_connection_string, parameters=parameters)
    ids = ml_processor.find_fitting_cvs(simple=True)

    return ids, ml_processor.error


def get_all_ids(parameters, mongo_connection_string):

    ml_processor = MLProcessor(uri=mongo_connection_string, parameters=parameters)
    ids = ml_processor.get_all_cvs()

    return ids, ml_processor.error


def set_cv_vacancy_labels(parameters, mongo_connection_string):

    ml_processor = MLProcessor(uri=mongo_connection_string, parameters=parameters)
    is_set = ml_processor.set_cv_vacancy_labels()

    return is_set, ml_processor.error
