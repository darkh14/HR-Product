from abc import ABCMeta, abstractmethod
import requests
from mongo_connection import MongoDBConnector
from time import sleep
from filter import Filter


class BaseParser:
    __metaclass__ = ABCMeta
    name = ''
    url = ''
    enable = False

    def __init__(self, **kwargs):
        super().__init__()
        self._set_url(**kwargs)
        self.dataset = list()
        self.status = ''
        self.error = ''
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

        self.filter_processor = Filter(**kwargs)
        self.params = {}

        self.cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                          'specialization', 'employment', 'work_schedule', 'seniority', 'experience',
                          'skills', 'education_level', 'education', 'resume_link', 'site_id', 'site_url', 'vacancy_id',
                          'profile_id', 'db', 'site']
        self.comp_fields = {'specialization': {'type': 'array'},
                            'seniority': {'type': 'dict', 'fields': ['years', 'months']},
                            'experience': {'type': 'array_dict', 'fields': ['start', 'end', 'timeinterval', 'company',
                                                                            'position', 'description']},
                            'skills': {'type': 'array'},
                            'education': {'type': 'array_dict', 'fields': ['final', 'name', 'organization']}}
        self.limit = kwargs.get('limit') or 0
        self.write_to = kwargs.get('write_to') or 'mongo'
        self.db_connector = kwargs.get('mongo_connector') or MongoDBConnector()
        self.kwargs = kwargs
        self.job = bool(kwargs.get('job'))
        self.vacancy_id = kwargs.get('vacancy_id') or ''
        self.profile_id = kwargs.get('profile_id') or ''
        self.db = kwargs.get('db') or ''
        self.sub_process = bool(kwargs.get('sub_process')) or False
        self.new_job_id = kwargs.get('new_job_id') or ''

        self._request_attempts = kwargs.get('request_attempts') or 100
        self._request_sleep = kwargs.get('request_sleep') or 0.5
        self._main_sleep = kwargs.get('main_sleep') or 1

    def parse(self,  parameters):

        if parameters.get('filter'):
            self._set_url_params(parameters['filter'])

        self.vacancy_id = parameters.get('vacancy_id') or self.vacancy_id
        self.profile_id = parameters.get('profile_id') or self.profile_id
        self.db = parameters.get('db') or self.db

        count = parameters.get('count') or 0
        limit = parameters.get('limit') or 0

        return self._parse_with_parameters(self.url, self.params, count, limit)

    @abstractmethod
    def _set_url(self, **kwargs):
        """method for setting url of starting page of parsing. this url transmits to function _parse_with_parameters
        as parameter"""

    @abstractmethod
    def _parse_with_parameters(self, url='', request_params=[], count=0, limit=0):
        """method for parsing site and saving result to DB or file"""

    def write_cv_data(self, cv_data):
        if self.write_to == 'mongo':
            self._write_cv_to_db(cv_data)
        elif self.write_to == 'json':
            self._write_cv_to_json(cv_data)
        else:
            self.error = 'Place to write data is not set'
            self.status = 'error'

    def _write_cv_to_db(self, cv_data):
        self.db_connector.write_cv_line(cv_data)

    def _write_cv_to_json(self, cv_data):
        self.dataset.append(cv_data)

    def _set_url_params(self, **kwargs):

        self.params = {}
        parsing_filter = kwargs.get('filter')
        if parsing_filter:
            for filter_key, filter_value in parsing_filter.items():
                self.params[filter_key] = self.filter_processor.get_filter_value(filter_value, filter_key,
                                                                                 self.__class__.name)

    def get_cv_data(self, path_el, **kwargs):
        cv_line = dict()
        for cv_field in self.cv_fields:
            if cv_field != '_id':
                value = self.get_cv_field(path_el, cv_field, **kwargs)
                cv_line[cv_field] = value
                if cv_field == 'seniority' and value:
                    cv_line['seniority_years'] = value.get('years')
                    cv_line['seniority_months'] = value.get('months')
        return cv_line

    def get_cv_field(self, path_el, field_name, **kwargs):

        field = ''
        if field_name == 'address':
            field = self.get_address(path_el, **kwargs)
        elif field_name == 'gender':
            field = self.get_gender(path_el, **kwargs)
        elif field_name == 'salary':
            field = self.get_salary(path_el, **kwargs)
        elif field_name == 'valuta':
            field = self.get_valuta(path_el, **kwargs)
        elif field_name == 'age':
            field = self.get_age(path_el, **kwargs)
        elif field_name == 'position':
            field = self.get_position(path_el, **kwargs)
        elif field_name == 'about_me':
            field = self.get_about_me(path_el, **kwargs)
        elif field_name == 'category':
            field = self.get_category(path_el, **kwargs)
        elif field_name == 'specialization':
            field = self.get_specialization(path_el, **kwargs)
        elif field_name == 'employment':
            field = self.get_employment(path_el, **kwargs)
        elif field_name == 'work_schedule':
            field = self.get_work_schedule(path_el, **kwargs)
        elif field_name == 'seniority':
            field = self.get_seniority(path_el, **kwargs)
        elif field_name == 'skills':
            field = self.get_skills(path_el, **kwargs)
        elif field_name == 'education_level':
            field = self.get_education_level(path_el, **kwargs)
        elif field_name == 'education':
            field = self.get_education(path_el, **kwargs)
        elif field_name == 'experience':
            field = self.get_experience(path_el, **kwargs)
        elif field_name == 'resume_link':
            field = self.get_resume_link(path_el, **kwargs)
        elif field_name == 'site_id':
            field = self.get_site_id(path_el, **kwargs)
        elif field_name == 'site_url':
            field = self.get_site_url(**kwargs)
        elif field_name == 'vacancy_id':
            field = self.get_vacancy_id(**kwargs)
        elif field_name == 'profile_id':
            field = self.get_profile_id(**kwargs)
        elif field_name == 'db':
            field = self.get_db(**kwargs)
        elif field_name == 'site':
            field = self.get_site(**kwargs)
        else:
            self.error = "Field name '{}' is not allowed".format(field_name)

        return field

    def _get_response(self, url, headers=None, params=None):

        attempts = self._request_attempts or 1

        response = None

        for _att in range(attempts):
            try:
                response = requests.get(url, headers=headers, params=params)
                break
            except TimeoutError:
                if self._request_sleep:
                    sleep(self._request_sleep)

        return response

    @abstractmethod
    def get_position(self, element, **kwargs):
        """returns value of position in current cv line"""

    @abstractmethod
    def get_address(self, element, **kwargs):
        """returns value of address in current cv line"""

    @abstractmethod
    def get_gender(self, element, **kwargs):
        """returns value of gender in current cv line"""

    @abstractmethod
    def get_salary(self, element, **kwargs):
        """returns value of salary in current cv line"""

    @abstractmethod
    def get_valuta(self, element, **kwargs):
        """returns value of valuta in current cv line"""

    @abstractmethod
    def get_age(self, element, **kwargs):
        """returns value of age in current cv line"""

    @abstractmethod
    def get_about_me(self, element, **kwargs):
        """returns value of about me in current cv line"""

    @abstractmethod
    def get_category(self, element, **kwargs):
        """returns value of category in current cv line"""

    @abstractmethod
    def get_specialization(self, element, **kwargs):
        """returns value of specialization in current cv line"""

    @abstractmethod
    def get_employment(self, element, **kwargs):
        """returns value of employment in current cv line"""

    @abstractmethod
    def get_work_schedule(self, element, **kwargs):
        """returns value of work schedule in current cv line"""

    @abstractmethod
    def get_seniority(self, element, **kwargs):
        """returns value of seniority in current cv line"""

    @abstractmethod
    def get_experience(self, element, **kwargs):
        """returns value of experience in current cv line"""

    @abstractmethod
    def get_skills(self, element, **kwargs):
        """returns value of skills in current cv line"""

    @abstractmethod
    def get_education_level(self, element, **kwargs):
        """returns value of education level in current cv line"""

    @abstractmethod
    def get_education(self, element, **kwargs):
        """returns value of education in current cv line"""

    @abstractmethod
    def get_resume_link(self, element, **kwargs):
        """returns resume link of current cv line"""

    @abstractmethod
    def get_site_id(self, element, **kwargs):
        """returns inner id of current cv line"""

    def get_site_url(self, **kwargs):
        return self.base_url

    def get_vacancy_id(self, **kwargs):
        return kwargs.get('vacancy_id') or self.vacancy_id

    def get_profile_id(self, **kwargs):
        return kwargs.get('profile_id') or self.profile_id

    def get_db(self, **kwargs):
        return kwargs.get('db') or self.db

    def get_site(self, **kwargs):
        return self.__class__.name

    @staticmethod
    def months_numbers() -> dict:
        result = dict()
        result['январь'] = 1
        result['февраль'] = 2
        result['март'] = 3
        result['апрель'] = 4
        result['май'] = 5
        result['июнь'] = 6
        result['июль'] = 7
        result['август'] = 8
        result['сентябрь'] = 9
        result['октябрь'] = 10
        result['ноябрь'] = 11
        result['декабрь'] = 12

        result['january'] = 1
        result['february'] = 2
        result['march'] = 3
        result['april'] = 4
        result['may'] = 5
        result['june'] = 6
        result['july'] = 7
        result['august'] = 8
        result['september'] = 9
        result['october'] = 10
        result['november'] = 11
        result['december'] = 12

        result['Қаңтар'.lower()] = 1
        result['Ақпан'.lower()] = 2
        result['Наурыз'.lower()] = 3
        result['Сәуір'.lower()] = 4
        result['Мамыр'.lower()] = 5
        result['Маусым'.lower()] = 6
        result['Шілде'.lower()] = 7
        result['Тамыз'.lower()] = 8
        result['Қыркүйек'.lower()] = 9
        result['Қазан'.lower()] = 10
        result['Қараша'.lower()] = 11
        result['Желтоқсан'.lower()] = 12

        result['Січень'.lower()] = 1
        result['Лютий'.lower()] = 2
        result['Березень'.lower()] = 3
        result['Квітень'.lower()] = 4
        result['Травень'.lower()] = 5
        result['Червень'.lower()] = 6
        result['Липень'.lower()] = 7
        result['Серпень'.lower()] = 8
        result['Вересень'.lower()] = 9
        result['Жовтень'.lower()] = 10
        result['Листопад'.lower()] = 11
        result['Грудень'.lower()] = 12

        result['Студзень'.lower()] = 1
        result['Люты'.lower()] = 2
        result['Сакавiк'.lower()] = 3
        result['Красавiк'.lower()] = 4
        result['Май'.lower()] = 5
        result['Травень'.lower()] = 5
        result['Чэрвень'.lower()] = 6
        result['Лiпень'.lower()] = 7
        result['Жнiвень'.lower()] = 8
        result['Верасень'.lower()] = 9
        result['Кастрычнiк'.lower()] = 10
        result['Лiстапад'.lower()] = 11
        result['Снежань'.lower()] = 12

        return result
