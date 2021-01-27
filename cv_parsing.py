from abc import ABCMeta, abstractmethod
import requests
from lxml import html
from mongo_connection import MongoDBConnector
import datetime
import pandas as pd
import uuid
import sys
import subprocess
import traceback
from urllib.parse import unquote
import sys


class BaseParser:
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        super().__init__()

        self.dataset = list()
        self.status = ''
        self.error = ''
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

        self.cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                          'specialization', 'еmployment', 'work_schedule', 'seniority', 'experience',
                          'skills', 'education_level', 'education', 'resume_link', 'site_id', 'site_url', 'vacancy_id',
                          'db']
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
        self.vacancy_id = kwargs.get('vacancy_id')
        self.db = kwargs.get('db')

    @abstractmethod
    def parse(self, parsing_filter=None):
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

    def get_cv_data(self, path_el, **kwargs):
        cv_line = dict()
        for cv_field in self.cv_fields:
            if cv_field != '_id':
                cv_line[cv_field] = self.get_cv_field(path_el, cv_field, **kwargs)
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
            field = self.get_resume_link(**kwargs)
        elif field_name == 'site_id':
            field = self.get_site_id(**kwargs)
        elif field_name == 'site_url':
            field = self.get_site_url(**kwargs)
        elif field_name == 'vacancy_id':
            field = self.get_vacancy_id(**kwargs)
        elif field_name == 'db':
            field = self.get_db(**kwargs)
        else:
            self.error = "Field name '{}' is not allowed".format(field_name)

        return field

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
    def get_resume_link(self, **kwargs):
        """returns resume link of current cv line"""

    @abstractmethod
    def get_site_id(self, **kwargs):
        """returns inner id of current cv line"""

    def get_site_url(self, **kwargs):
        return self.base_url

    def get_vacancy_id(self, **kwargs):
        return kwargs.get('vacancy_id') or self.vacancy_id

    def get_db(self, **kwargs):
        return kwargs.get('db') or self.db

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


class HeadHunterParser(BaseParser):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = kwargs.get('base_url') or 'https://hh.ru'
        self.url = self.base_url + (kwargs.get('add_url') or '/search/resume')

        self.params = []

        self._url_params_to_add = {'st': 'resumeSearch',
                                   'logic': 'normal',
                                   'pos': 'full_text',
                                   'exp_period': 'all_time',
                                   'exp_company_size': 'any'}
        self.edu_map = {'Не имеет значения': 'none',
                        'Высшее': 'higher',
                        'Бакалавр': 'bachelor',
                        'Магистр': 'master',
                        'Кандидат наук': 'candidate',
                        'Доктор наук': 'doctor',
                        'Незаконченное высшее': 'unfinished_higher',
                        'Среднее': 'secondary',
                        'Среднее специальное': 'special_secondary'
                        }
        self.parsing_filter = kwargs.get('parsing_filter') or {'text': r'Программист 1с'}
        self._set_url_params()

        self.kwargs.update(kwargs)

        self.current_salary_data = None
        self.current_link = ''

    def parse(self, parsing_filter=None):

        current_url = self.url

        counter = 1
        page_counter = 1

        if parsing_filter:
            self._set_url_params(parsing_filter)

        params = self.params.copy()

        stop = False

        while current_url:

            response = requests.get(current_url, headers=self.headers, params=params)

            root = html.fromstring(response.text)

            cv_links = root.xpath("//a[@class='resume-search-item__name']/@href")

            if not self.job:
                print('Page ' + str(page_counter))
                print(unquote(response.url))

            for cv_link in cv_links:

                if self.limit and counter > self.limit:
                    stop = True
                    break

                response = requests.get(self.base_url + cv_link, headers=self.headers)
                cv_root = html.fromstring(response.text)

                self.current_link = (self.base_url + cv_link).split('?')[0]

                cv_data = self.get_cv_data(cv_root, cv_link=self.current_link)

                self.write_cv_data(cv_data)

                self.current_salary_data = None
                if not self.job:
                    print('Cv ' + str(counter) + ': ' + self.current_link)

                counter += 1

            if stop:
                break

            page_counter += 1

            params = {}
            href_list = root.xpath(
                        "//div[@data-qa='pager-block']/span[@class='bloko-form-spacer']/a[@class='bloko-button']/@href")

            current_url = href_list and self.base_url + href_list[0]

        if self.write_to == 'json':
            df = pd.DataFrame(self.dataset)
            df.to_json('cv.json', orient='records', lines=True, force_ascii=False)

        self.status = 'OK'

        return True

    def _set_url_params(self, parsing_filter=None, set_parsing_filter=False):

        if not parsing_filter:
            parsing_filter = self.parsing_filter

        self.params = []

        text_pars_added = False
        if parsing_filter.get('texts'):
            for text_par in parsing_filter.get('texts'):
                self.params.append(('text', text_par))

                for ad_par, ad_value in self._url_params_to_add.items():
                    val = self._get_url_param(ad_par, parsing_filter) or ad_value
                    self.params.append((ad_par, val))

                text_pars_added = True

        for key, value in parsing_filter.items():
            if not self._url_params_to_add.get(key) or not text_pars_added:
                par_value = self._get_url_param(key, parsing_filter)
                self.params.append((key, par_value))

        additional = {'order_by': 'relevance', 'search_period': '0', 'items_on_page': '50', 'no_magic': 'false'}

        for ad_par, ad_val in additional.items():
            if not parsing_filter.get(ad_par):
                self.params.append((ad_par, ad_val))

        if set_parsing_filter:
            self.parsing_filter = parsing_filter

    def _get_url_param(self, param_name, param_dict):

        result = None

        if param_name == 'education':
            if param_dict.get(param_name):
                result = self.edu_map.get(param_dict.get(param_name))

        # specialization - to do!

        if not result:
            result = param_dict.get(param_name)

        return result

    def get_position(self, element, **kwargs):
        data = element.xpath(
            "//span[@class='resume-block__title-text' and @data-qa='resume-block-title-position']/text()")
        data = data[0] if data else ''

        return data

    def get_address(self, element, **kwargs):
        data = element.xpath("//span[@data-qa='resume-personal-address']/text()")
        data = data[0] if data else ''

        return data

    def get_gender(self, element, **kwargs):
        data = element.xpath("//span[@data-qa='resume-personal-gender']/text()")
        data = data[0] if data else ''

        return data

    def set_salary_data(self, element):
        data_list = element.xpath("//span[@data-qa='resume-block-salary']//text()")

        data = {}
        if len(data_list) != 3:
            data['salary'] = 0
            data['currency'] = ''
        else:
            data['salary'] = int(data_list[0].replace(u'\u2009', ''))
            data['currency'] = data_list[2]

        self.current_salary_data = data

    def get_salary(self, element, **kwargs):
        if not self.current_salary_data:
            self.set_salary_data(element)

        return self.current_salary_data['salary']

    def get_valuta(self, element, **kwargs):
        if not self.current_salary_data:
            self.set_salary_data(element)

        return self.current_salary_data['currency']

    def get_age(self, element, **kwargs):
        data = element.xpath("//span[@data-qa='resume-personal-age']/text()")
        data = int(data[0]) if data else 0

        return data

    def get_about_me(self, element, **kwargs):
        data = element.xpath("//div[@data-qa='resume-block-skills-content']//text()")
        data = data[0] if data else ''
        data = data.replace(u'\r', ' ').replace(u'\n', ' ')

        return data

    def get_category(self, element, **kwargs):
        data = element.xpath("//span[@data-qa='resume-block-specialization-category']/text()")
        data = data[0] if data else ''

        return data

    def get_specialization(self, element, **kwargs):
        data = element.xpath("//li[@data-qa='resume-block-position-specialization']/text()")

        return data

    def get_employment(self, element, **kwargs):
        data = element.xpath(
            "//div[@class='resume-block-container']//div[@class='bloko-gap bloko-gap_bottom']/../p[1]/text()")

        data.pop(0)

        data = data[0] if data else ''

        return data

    def get_work_schedule(self, element, **kwargs):
        data = element.xpath(
            "//div[@class='resume-block-container']//div[@class='bloko-gap bloko-gap_bottom']/../p[2]/text()")

        data.pop(0)

        data = data[0] if data else ''

        return data

    def get_seniority(self, element, **kwargs):
        data = element.xpath("//span[@class='resume-block__title-text resume-block__title-text_sub']/text()")

        not_del_list = ['лет', 'года', 'год', 'месяц', 'месяца', 'месяцев']

        c_data = data.copy()

        for data_element in c_data:
            if not data_element.isdigit() and not data_element in not_del_list:
                data.remove(data_element)

        result = {}
        if len(data) == 4:
            result['years'] = data[0]
            result['months'] = data[2]
        elif len(data) == 2:
            if data[1] in ['лет', 'год', 'года']:
                result['years'] = data[0]
                result['months'] = 0
            else:
                result['years'] = 0
                result['months'] = data[0]
        else:
            result['years'] = 0
            result['months'] = 0

        return result

    def get_experience(self, element, **kwargs):
        result = []
        blocks = element.xpath("//div[@data-qa='resume-block-experience']/*/*/div[@class='resume-block-item-gap']")
        for block in blocks:
            record = self.get_experience_record(block)
            result.append(record)

        return result

    def get_experience_record(self, element):
        result = {}

        data = element.xpath(
            ".//div[@class='bloko-column bloko-column_xs-4 bloko-column_s-2 bloko-column_m-2 bloko-column_l-2']//text()")
        # print(data)
        start_date = '01-' + "{:02d}".format((self.months_numbers().get(data[0].lower()))) + '-' + data[2]

        for ind in range(4):
            data.pop(0)

        if data[0] in ['по\xa0настоящее\xa0время', 'currently', 'по\xa0теперішній\xa0час', 'қазіргі уақытқа дейін']:
            end_date = datetime.datetime.today().strftime("01-%m-%Y")
            data.pop(0)
        else:
            end_date = '01-' + "{:02d}".format((self.months_numbers().get(data[0].lower()))) + '-' + data[2]

            for ind in range(3):
                data.pop(0)

        result['start'] = start_date
        result['end'] = end_date

        while '\xa0' in data:
            data.remove('\xa0')
        while ' ' in data:
            data.remove(' ')

        timeinterval = {}

        if len(data) == 4:
            timeinterval['years'] = data[0]
            timeinterval['months'] = data[2]
        elif len(data) == 2:
            if data[1] in ['лет', 'год', 'года']:
                timeinterval['years'] = data[0]
                timeinterval['months'] = 0
            else:
                timeinterval['years'] = 0
                timeinterval['months'] = data[0]
        else:
            timeinterval['years'] = 0
            timeinterval['months'] = 0

        result['timeinterval'] = timeinterval

        data = element.xpath(".//div[@class='bloko-text-emphasis']//text()")

        if len(data) == 2:
            result['company'] = data[0]
            result['position'] = data[1]
        elif len(data) == 1:
            data_0 = data[0]
            position = element.xpath(".//div[@data-qa='resume-block-experience-position']//text()")

            if data_0 == position:
                result['company'] = ''
                result['position'] = data_0
            else:
                result['company'] = data_0
                result['position'] = ''

        data = element.xpath(".//div[@data-qa='resume-block-experience-description']/text()")

        data = data[0] if data else ''
        data = data.replace(u'\r', ' ').replace(u'\n', ' ')
        result['description'] = data

        return result

    def get_skills(self, element, **kwargs):
        data = element.xpath("//span[@class='bloko-tag__section bloko-tag__section_text']//text()")
        return data

    def get_education_level(self, element, **kwargs):
        data = element.xpath(
            "//div[@data-qa='resume-block-education']//span[@class='resume-block__title-text resume-block__title-text_sub']//text()")
        data = data[0] if data else ''
        return data

    def get_education(self, element, **kwargs):
        result = []
        blocks = element.xpath("//div[@data-qa='resume-block-education']/*/*/div[@class='resume-block-item-gap']")
        for block in blocks:
            record = self.get_education_record(block)
            result.append(record)

        return result

    def get_education_record(self, element):
        result = {}
        data = element.xpath("./div/div[1]/text()")
        data = int(data[0]) if data else 0
        result['final'] = data
        data = element.xpath(
            ".//div[@class='bloko-column bloko-column_xs-4 bloko-column_s-6 bloko-column_m-7 bloko-column_l-10']//text()")

        result['name'] = data[0]
        if len(data) > 1:
            result['organization'] = data[1]
        else:
            result['organization'] = ''
        if len(data) == 4:
            result['specialization'] = data[3]
        else:
            result['specialization'] = ''

        return result

    def get_resume_link(self, **kwargs):

        cv_link = kwargs.get('cv_link')

        if cv_link:
            return (self.base_url + cv_link).split('?')[0]
        else:
            return ''

    def get_site_id(self, **kwargs):

        cv_link = kwargs.get('cv_link')

        if cv_link:
            return (self.base_url + cv_link).split('?')[0].split('/')[-1]
        else:
            return ''


class ParsingTool:

    def __init__(self, **kwargs):

        job = kwargs.get('job')
        if job is not None:
            self.job = job
        else:
            self.job = True

        self.new_job_id = kwargs.get('new_job_id') or ''
        self.status = ''
        self.error = ''

        self.site_parsers = {'hh.ru': HeadHunterParser}

        self.site_list = kwargs.get('site_list') or self.site_parsers.keys()

        self.kwargs = kwargs
        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()
        self.parsing_filter = kwargs.get('parsing_filter') or {}

    def parse(self, **kwargs):

        parsing_par = self.kwargs.copy()
        parsing_par.update(kwargs)

        new_job_id = str(uuid.uuid4())

        if self.job:

            job_name = 'refill_cv_collection'

            job_line = self.db_connector.read_job({'job': job_name, 'status': 'started'})

            if not job_line:
                new_line = {'job_id': new_job_id, 'job': job_name, 'status': 'created',
                            'parameters': parsing_par, 'error': ''}

                self.db_connector.write_job(new_line, ['job_id', 'job'])

                if sys.platform == "linux" or sys.platform == "linux2":
                    command = 'python3'
                else:
                    command = 'python'

                p = subprocess.Popen([command, 'cv_parsing.py', '-job', 'refill_cv_collection', new_job_id])

                self.status = 'OK'

            else:
                self.status = 'error'
                self.error = 'The job {} is already started and not finished'.format(job_name)
        else:
            for site in self.site_list:
                site_parser = self.site_parsers.get(site)(**parsing_par, db_connector=self.db_connector)

                result = site_parser.parse()

                if result:
                    self.status = 'OK'
                else:
                    self.status = 'error'
                    self.error = 'Site parser {} '.format(site) + ' parsing error. ' + site_parser.error

        return new_job_id


def refill_cv_collection(**kwargs):

    parsing_tool = ParsingTool(**kwargs)
    j_id = parsing_tool.parse()

    return j_id, parsing_tool.status, parsing_tool.error


if __name__ == '__main__':

    if len(sys.argv) == 4:
        if sys.argv[1] == '-job':

            job_id = str(uuid.UUID(sys.argv[3]))

            db_connector = MongoDBConnector()

            job_line = db_connector.read_job({'job': sys.argv[2], 'job_id': job_id})

            error_text = ''

            if job_line and job_line['status'] == 'created':

                job_line['status'] = 'started'
                db_connector.write_job(job_line, ['job_id', 'job'])

                parsing_parameters = job_line['parameters'].copy()

                error = False
                if parsing_parameters:
                    parsing_parameters['db_connector'] = db_connector
                else:
                    parsing_parameters = {'db_connector': db_connector}
                try:
                    parser = HeadHunterParser(**parsing_parameters)
                    parser.parse()
                except Exception as exc:
                    error = True
                    error_text = str(traceback.format_exc())

                job_line['status'] = 'error' if error else 'finished'
                job_line['error'] = error_text
                db_connector.write_job(job_line, ['job_id', 'job'])






