from lxml import html
import datetime
import pandas as pd
from urllib.parse import unquote
from time import sleep
from base_parser import BaseParser


class HeadHunterParser(BaseParser):

    name = 'HeadHunter'
    url = 'headhunter.ru'
    enable = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.params = []
        self.parsing_filter = kwargs.get('filter') or {'text': 'Программист 1с'}

        self._url_params_to_add = {'st': 'resumeSearch',
                                   'logic': 'normal',
                                   'pos': 'full_text',
                                   'exp_period': 'all_time',
                                   'exp_company_size': 'any'}

        self._set_url_params()

        self.kwargs.update(kwargs)

        self.current_salary_data = None
        self.current_link = ''


    def _set_url(self, **kwargs):

        self.base_url = kwargs.get('base_url') or 'https://hh.ru'
        self.url = self.base_url + (kwargs.get('add_url') or '/search/resume')

    def _parse_with_parameters(self, url='', request_params={}, count=0, limit=0):

        page_counter = 1
        stop = False

        current_url = url

        params = request_params.copy() or self.params

        while current_url:

            response = self._get_response(current_url, headers=self.headers, params=params)

            root = html.fromstring(response.text)

            cv_links = root.xpath("//a[@class='resume-search-item__name']/@href")

            if not self.job and not self.sub_process:
                print('Page ' + str(page_counter))
                print(unquote(response.url))

            for cv_link in cv_links:

                if limit and count >= limit:
                    stop = True
                    break

                response = self._get_response(self.base_url + cv_link, headers=self.headers)
                cv_root = html.fromstring(response.text)

                self.current_link = (self.base_url + cv_link).split('?')[0]

                cv_data = self.get_cv_data(cv_root, cv_link=self.current_link)

                self.write_cv_data(cv_data)

                self.current_salary_data = None

                count += 1

                if not self.job and not self.sub_process:
                    print('Cv ' + str(count) + ': ' + self.current_link)

                if self.sub_process and count % 10 == 0:
                    job_line = self.db_connector.read_job({'job': 'refill_cv_collection', 'job_id': self.new_job_id})
                    if job_line:
                        job_line['info'] = 'vacancy_id = {0}, db = {1}, cv_processed {2}'.format(self.vacancy_id,
                                                                                                 self.db, count)
                        self.db_connector.write_job(job_line, ['job_id'])

                if self._main_sleep:
                    sleep(self._main_sleep)

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

        return count

    def _set_url_params(self, parsing_filter=None):

        self.params = {}

        if not parsing_filter:
            parsing_filter = self.parsing_filter

        self.params.update(self._url_params_to_add)
        additional = {'order_by': 'relevance', 'items_on_page': '50', 'no_magic': 'false'}
        self.params.update(additional)

        for ad_par, ad_val in parsing_filter.items():
            parameter_value = self.filter_processor.get_filter_value(ad_val, ad_par, self.name)
            self.params[ad_par] = parameter_value

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
            if not data_element.isdigit() and data_element not in not_del_list:
                data.remove(data_element)

        result = {}
        if len(data) == 4:
            result['years'] = int(data[0])
            result['months'] = int(data[2])
        elif len(data) == 2:
            if data[1] in ['лет', 'год', 'года', 'year', 'years', 'рок']:
                result['years'] = int(data[0])
                result['months'] = 0
            else:
                result['years'] = 0
                result['months'] = int(data[0])
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

    def get_resume_link(self, element, **kwargs):

        cv_link = kwargs.get('cv_link')

        if cv_link:
            return (self.base_url + cv_link).split('?')[0]
        else:
            return ''

    def get_site_id(self, element, **kwargs):

        cv_link = kwargs.get('cv_link')

        if cv_link:
            return (self.base_url + cv_link).split('?')[0].split('/')[-1]
        else:
            return ''
