from mongo_connection import MongoDBConnector
import difflib


class MLProcessor(MongoDBConnector):

    def __init__(self, uri='', filter_parameters=None):
        super().__init__(uri=uri)
        self._cv = []
        self._fitting_cv = []
        self.error = ''
        self._cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                           'specialization', 'resume_link']
        self._filter_parameters_names = ['AgeBegin', 'AgeEnd', 'Gender', 'WorkExperienceBegin',
                                         'WorkExperienceEnd', 'Education', 'Requirements', 'Duties']
        self._filter_is_set = False
        self._filter_parameters = dict.fromkeys(self._filter_parameters_names, '')
        self.threshold = 0.565

        if filter_parameters:
            self._set_filter(filter_parameters)
            if filter_parameters.get('Threshold'):
                self.threshold = filter_parameters.get('Threshold')

    def _set_filter(self, filter_parameters):
        self._filter_is_set = False
        for name in self._filter_parameters_names:
            par = filter_parameters.get(name)
            if par:
                self._filter_parameters[name] = par

        self._filter_is_set = True

    def find_fitting_cvs(self, parameters=None, simple=False):

        if parameters:
            self._set_filter(parameters)

        if simple:
            cvs = self._simple_find_fitting_cvs(parameters)
        else:
            cvs = []  # neural network
        return cvs

    def _simple_find_fitting_cvs(self, parameters, mongo_connection_string=''):

        if parameters:
            self._set_filter(parameters)

        self.connect(uri=mongo_connection_string)

        if self.is_connected:
            self._cv = self.get_cv()

            for cv_line in self._cv.find():
                fits = self._check_cv_row(cv_line)
                if fits:
                    cur_line = {}
                    for name in self._cv_fields:
                        if name == '_id':
                            cur_line[name] = str(cv_line.get(name))
                        else:
                            cur_line[name] = cv_line.get(name)

                    self._fitting_cv.append(cur_line)

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
            elif (par_name == 'WorkExperienceBegin' or par_name == 'WorkExperienceEnd') and not work_experience_processed:
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

    def _process_range_parameter(self, value, par_begin, par_end, endnul_as_inf=False):
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
        elif value >= par_begin and value <= par_end:
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


def find_fitting_ids(parameters, mongo_connection_string):

    ml_processor = MLProcessor(uri=mongo_connection_string, filter_parameters=parameters)
    ids = ml_processor.find_fitting_cvs(simple=True)

    print(len(ids))
    return ids, ml_processor.error

