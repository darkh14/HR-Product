from abc import ABCMeta, abstractmethod
import requests
from lxml import html
from mongo_connection import MongoDBConnector
import datetime
import pandas as pd


class BaseParser(MongoDBConnector):
    __metaclass__ = ABCMeta

    def __init__(self, mongo_uri='', **kwargs):
        super().__init__(uri=mongo_uri)
        self.base_url = ''
        self.url = ''
        self.dataset = list
        self.status = ''
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

        self.cv_fields = ['_id', 'address', 'gender', 'salary', 'valuta', 'age', 'position', 'about_me', 'category',
                          'specialization', 'еmployment', 'work_schedule', 'seniority', 'experience',
                          'skills', 'education_level', 'education', 'resume_link', 'site_id', 'site_url']
        self.comp_fields = {'specialization': {'type': 'array'},
                            'seniority': {'type': 'dict', 'fields': ['years', 'months']},
                            'experience': {'type': 'array_dict', 'fields': ['start', 'end', 'timeinterval', 'company',
                                                                            'position', 'description']},
                            'skills': {'type': 'array'},
                            'education': {'type': 'array_dict', 'fields': ['final', 'name', 'organization']}}
        self.first_lines = kwargs.get('first_lines') if kwargs.get('first_lines') else 0
        self.write_to = kwargs.get('write_to') if kwargs.get('write_to') else 'mongo'

    @abstractmethod
    def parse(self, parse_filter=None):
        """method for parsing site and saving result to mongo DB"""

    def write_cv_data(self, cv_data):
        if self.write_to == 'mongo':
            if not self.is_connected:
                self.connect()
            self._write_cv_to_db(cv_data)
        elif self.write_to == 'json':
            self._write_cv_to_json(cv_data)
        else:
            self.error = 'Place to write data is not set'
            self.status = 'error'

    def _write_cv_to_db(self, cv_data):
        self.write_cv_line(cv_data)

    def _write_cv_to_json(self, cv_data):
        self.dataset.append(cv_data)

    def get_cv_data(self, path_el, **kwargs):
        line = dict()
        for cv_field in self.cv_fields:
            if cv_field != '_id':
                line[cv_field] = self.get_cv_field(path_el, cv_field, **kwargs)
        return line

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
        else:
            self.error = 'Field name {} is not allowed'.format(field_name)

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

    def __init__(self, mongo_uri='', **kwargs):
        super().__init__(mongo_uri, **kwargs)
        self.base_url = 'https://hh.ru'
        self.url = self.base_url + '/search/resume'
        self.params = {'exp_period': 'all_time', 'logic': 'normal', 'order_by': 'relevance',
                       'pos': 'full_text', 'text': r'Программист 1с'}
        if kwargs.get('searching_text'):
            self.params['text'] = kwargs.get('searching_text')
        self.current_salary_data = None
        self.current_link = ''

    def parse(self, parse_filter=None):

        current_url = self.url

        counter = 1
        page_counter = 1

        params = self.params
        stop = False

        while current_url:

            response = requests.get(current_url, headers=self.headers, params=params)

            root = html.fromstring(response.text)

            cv_links = root.xpath("//a[@class='resume-search-item__name']/@href")

            print('Page ' + str(page_counter))

            for cv_link in cv_links:

                if self.first_lines and counter > self.first_lines:
                    stop = True
                    break

                response = requests.get(self.base_url + cv_link, headers=self.headers)
                cv_root = html.fromstring(response.text)

                self.current_link = (self.base_url + cv_link).split('?')[0]

                cv_data = self.get_cv_data(cv_root, cv_link=self.current_link)

                self.write_cv_data(cv_data)

                self.current_salary_data = None
                print('Cv ' + str(counter) + ': ' + self.current_link)

                counter += 1

            if stop:
                break

            page_counter +=1

            params = {}
            href_list = root.xpath(
                        "//div[@data-qa='pager-block']/span[@class='bloko-form-spacer']/a[@class='bloko-button']/@href")

            current_url = href_list and self.base_url + href_list[0]

        if self.write_to == 'json':
            df = pd.DataFrame(self.dataset)
            df.to_json('cv.json', orient='records', lines=True, force_ascii=False)

        self.status = 'OK'

        return True

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

    def __init__(self, site_list=None, parameters=None, mongo_uri=''):
        self.status = ''
        self.error = ''
        self.available_sites = ['hh.ru']
        self.site_list = site_list or ['hh.ru']
        self.site_parsers = {'hh.ru': HeadHunterParser}
        self.parameters = parameters
        self.mongo_uri = mongo_uri

    def _check_sites(self):
        for site in self.site_list:
            if site not in self.available_sites:
                self.status = 'error'
                self.error = 'Site {} is not available'.format(site)
                break

        return self.status != 'error'

    def parse(self):
        for site in self.site_list:
            site_parser = self.site_parsers.get(site)(mongo_uri=self.mongo_uri, **self.parameters)

            result = site_parser.parse()

            if result:
                self.status = 'OK'
            else:
                self.status = 'error'
                self.error = 'Site parser {} '.format(site) + ' parsing error. ' + site_parser.error

        return self.status == 'OK'


def refill_cv_collection(parameters, mongo_connection_string):

    parsing_tool = ParsingTool(mongo_uri=mongo_connection_string, parameters=parameters)
    result = parsing_tool.parse()

    return parsing_tool.status, parsing_tool.error


if __name__ == '__main__':

    parser = HeadHunterParser()
    print(parser.base_url)

