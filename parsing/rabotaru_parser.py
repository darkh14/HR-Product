from lxml import html
from bs4 import BeautifulSoup as bs
import pandas as pd
from time import sleep
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from base_parser import BaseParser


class RabotaRuParser(BaseParser):

    name = 'RabotaRu'
    url = 'rabota.ru'
    enable = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.params = []
        self.parsing_filter = kwargs.get('filter') or {'text': 'Программист 1с'}

        self._url_params_to_add = {'action': 'search', 'area': 'v3_searchResumeByParamsResults', 'p': -2005, 'w': '',
                  'qot[0]': 3, 'qsa[0][]': 1, 'sf': '', 'st': '', 'cu': 2, 'krl[]': 3, 'af': '', 'at': '', 'sex': '',
                  'eylo': '', 't2l': '', 'nex': True}

        self._set_url_params()
        self._web_driver = None
        self._web_driver_is_set = False

        self.kwargs.update(kwargs)
        self._page_sleep_interval = 2

        self._gender_age_str = ''
        self._current_salary_data = None
        self._education_data = None

    def _set_url(self, **kwargs):

        self.base_url = kwargs.get('base_url') or 'https://www.rabota.ru'
        self.url = self.base_url + (kwargs.get('add_url') or '/v3_searchResumeByParamsResults.html')

    def _parse_with_parameters(self, url='', request_params={}, count=0, limit=0):

        params = request_params.copy() or []

        response = self._get_response(url, headers=self.headers, params=params)

        if response:

            html_start = bs(response.text, 'lxml')
            amount_list = html_start.find('div', {'class': 'b-search-res-result'}).getText().split(' ')
            if amount_list[3] == 'резюме':
                sum_ = int(amount_list[1] + amount_list[2])
            else:
                sum_ = int(amount_list[1])

            pages_count = int(sum_ // 20) + (0 if sum_ % 20 == 0 else 1)
            pages_limit = int(limit / 20) + (0 if limit % 20 == 0 else 1)
            if pages_limit:
                pages_count = min(pages_count, pages_limit)

            if pages_count > 1:
                html_page = self._get_full_html(response.url, pages_count)
            else:
                html_page = response.text

            html = bs(html_page, 'lxml')
            cv_elements = html.find_all('a', {'class': 'js-follow-link-ignore box-wrapper__resume-name'})

            if cv_elements:
                for cv_element in cv_elements:

                    if limit and count >= limit:
                        break

                    url_form = cv_element.get('href')

                    response_form = self._get_response(url_form, self.headers)

                    if not response_form:
                        continue

                    html_form = bs(response_form.text, 'lxml')

                    cv_data = self.get_cv_data(html_form, cv_link=url_form)

                    self.write_cv_data(cv_data)

                    count += 1

                    if not self.job and not self.sub_process:
                        print('Cv ' + str(count) + ': ' + url_form)

                    if self.sub_process and count % 10 == 0:
                        job_line = self.db_connector.read_job({'job': 'refill_cv_collection',
                                                               'job_id': self.new_job_id})
                        if job_line:
                            job_line['info'] = 'vacancy_id = {0}, db = {1}, cv_processed {2}'.format(self.vacancy_id,
                                                                                                     self.db, count)
                            self.db_connector.write_job(job_line, ['job_id'])

        if self.write_to == 'json':
            df = pd.DataFrame(self.dataset)
            df.to_json('cv.json', orient='records', lines=True, force_ascii=False)

        self.status = 'OK'

        if self._web_driver_is_set:
            self._web_driver.close()

        return count

    def get_cv_data(self, cv_element, **kwargs):

        result = super(RabotaRuParser, self).get_cv_data(cv_element, **kwargs)
        self._gender_age_str = ''
        self._current_salary_data = None
        self._education_data = None

        return result

    def _set_url_params(self, parsing_filter=None):

        self.params = {}

        if not parsing_filter:
            parsing_filter = self.parsing_filter

        self.params.update(self._url_params_to_add)

        for ad_par, ad_val in parsing_filter.items():
            parameter_name = self.filter_processor.get_filter_value(ad_par, 'settings', self.name)
            parameter_value = self.filter_processor.get_filter_value(ad_val, ad_par, self.name)
            self.params[parameter_name] = parameter_value

    def _set_web_driver(self):
        if not self._web_driver_is_set:
            executable_path = GeckoDriverManager(print_first_line=False).install()

            options = webdriver.FirefoxOptions()
            options.add_argument('--headless')

            self._web_driver = webdriver.Firefox(executable_path=executable_path,
                                                 # log_path='/home/mladmin/smt/geckodriver.log',
                                                 options=options)
            self._web_driver_is_set = True

    def _delete_web_driver(self):
        if self._web_driver_is_set:
            self._web_driver.close()
            self._web_driver_is_set = False

    def _get_full_html(self, url, pages_count):

        self._set_web_driver()
        self._web_driver.get(url)

        for page_number in range(pages_count - 1):
            # sleep(self._page_sleep_interval)
            # elem = self._web_driver.find_elements_by_class_name("resume-search-short-list")[-1] - scroll not always works

            sleep(self._page_sleep_interval)

            elem = self._web_driver.find_element_by_xpath(
                "(//a[@class='js-follow-link-ignore box-wrapper__resume-name'])[last()]") # - more stable
            elem.click()

            html_page = self._web_driver.page_source
            root = html.fromstring(html_page)
            elements = root.xpath("//a[@class='js-follow-link-ignore box-wrapper__resume-name']")
            if not self.job and not self.sub_process:
                print('Lines loaded: {}'.format(len(elements)))
            if self.sub_process and page_number % 10 == 0:
                job_line = self.db_connector.read_job({'job': 'refill_cv_collection',
                                                       'job_id': self.new_job_id})
                if job_line:
                    job_line['info'] = 'vacancy_id = {0}, db = {1}, cv_html_loaded {2}'\
                        .format(self.vacancy_id, self.db, len(elements))
                    self.db_connector.write_job(job_line, ['job_id'])

        result = self._web_driver.page_source

        self._delete_web_driver()

        return result

    def get_position(self, element, **kwargs):
        data = element.find('span', {'class': 'text_24 bold position-name'}).getText()\
            .replace('\t', '').replace('\n', '')

        return data

    def get_address(self, element, **kwargs):
        data = element.find('p', {'class': 'b-city-info mt_10'}).getText()

        return data

    def get_gender(self, element, **kwargs):
        if not self._gender_age_str:
            self._gender_age_str = element.find('p', {'class': 'b-sex-age'}).getText().replace('\t', '').replace('\n', '')
        if len(self._gender_age_str.split(',')) > 1:
            data = self._gender_age_str[8:]
        else:
            data = self._gender_age_str

        return data

    def get_age(self, element, **kwargs):
        if not self._gender_age_str:
            self._gender_age_str = element.find('p', {'class': 'b-sex-age'}).getText().replace('\t', '').replace('\n', '')
        if len(self._gender_age_str.split(',')) > 1:
            data = int(self._gender_age_str[:2])
        else:
            data = 0

        return data

    def set_salary_data(self, element):

        salary = ''
        valuta = ''

        if (element.find('span', {'class': 'text_24 salary nobr'}).getText() and
                element.find('span', {'class': 'text_24 salary nobr'}).getText() != 'по договоренности'):

            salary_split = element.find('span', {'class': 'text_24 salary nobr'}).getText().split(' ')
            valuta = salary_split[-1].replace('.', '')

            for i in range(len(salary_split) - 1):
                salary += salary_split[i]

        if salary:
            salary = int(salary)
        else:
            salary = 0

        self._current_salary_data = {'salary': salary, 'currency': valuta}

        return self._current_salary_data

    def get_salary(self, element, **kwargs):
        if not self._current_salary_data:
            self.set_salary_data(element)

        return self._current_salary_data['salary']

    def get_valuta(self, element, **kwargs):
        if not self._current_salary_data:
            self.set_salary_data(element)

        return self._current_salary_data['currency']

    def get_about_me(self, element, **kwargs):

        if element.find('p', {'class': 'mt_4 p-res-qua lh_20 aboutme-info'}):
            data = element.find('p', {'class': 'mt_4 p-res-qua lh_20 aboutme-info'}).getText()\
                .replace('\t','').replace('\n','')
        else:
            data = ''

        return data

    def get_category(self, element, **kwargs):
        data = ''

        return data

    def get_specialization(self, element, **kwargs):

        if element.find('p', {'class': 'mb_10 lh_20'}):
            data = element.find('p', {'class': 'mb_10 lh_20'}).getText().replace('\t', '')\
                .replace('\n', '').split(',')
        else:
            data = []

        return data

    def get_employment(self, element, **kwargs):
        data = element.find('div', {'class': 'pt12 lh_20 p-fs16 td2'}).getText().replace('\t', '')\
            .replace('\n', '').split('.')[0]
        return data

    def get_work_schedule(self, element, **kwargs):
        data = element.find('div', {'class': 'pt12 lh_20 p-fs16 td2'}).getText()\
            .replace('\t', '').replace('\n', '')

        return data

    def get_seniority(self, element, **kwargs):

        if element.find('span', {'class': 'text_18 bold exp-years'}):
            seniority_str = element.find('span', {'class': 'text_18 bold exp-years'}).getText()
            if seniority_str.lower() == 'нет опыта':
                data = {'years': 0, 'months': 0}
            else:
                seniority_list = seniority_str.split(' ')
                years = 0
                months = 0
                if len(seniority_list) == 4:
                    years = int(seniority_list[0])
                    if seniority_list[2] == 'и':
                        months = 0
                    else:
                        months = int(seniority_list[2])
                elif len(seniority_list) == 2:
                    if seniority_list[1] in ['мес']:
                        months = int(seniority_list[0])
                    elif seniority_list[0] != 'Менее':
                        years = int(seniority_list[0])
                data = {'years': years, 'months': months}
        else:
            data = {'years': 0, 'months': 0}

        return data

    def get_experience(self, element, **kwargs):
        result = []
        blocks = element.find_all('div', {'class': 'res-card-tbl-row'})
        for block in blocks:
            record = self.get_experience_record(block)
            if record:
                result.append(record)

        return result

    def get_experience_record(self, element):
        exp_dict = {}
        if element.find('div', {'class': 'b-work-period'}):  # Потому что по мудацки названы классы
            # Парсим по мудацки собранный период работы
            work_period = element.find('p', {'class': 'b-work-period__years'}).getText().replace('\t', '')\
                .replace('\n', '')
            separator = work_period.find('—')

            year_start = work_period[:separator].split(',')[0]
            month_start = work_period[:separator].split(',')[1].replace(' ', '')
            month_end_number = self.months_numbers().get(month_start.lower())

            exp_dict['start'] = '01' + '-' + str(month_end_number) + '-' + str(year_start)

            year_end = work_period[separator + 1:].split(',')[0]
            month_end = work_period[separator:].split(',')[1].replace(' ', '')
            if len(element.find('p', {'class': 'b-work-period__years'}).getText().replace('\t', '').replace('\n', '')[separator:].split(',')) == 3:

                time_interval = {}
                exp_line = element.find('p', {'class': 'b-work-period__years'}).getText()
                exp_line = exp_line.replace('\t', '').replace('\n', '')[separator:].split(',')[2]

                sep_months = exp_line.find('м')
                if exp_line.find('г') < 0 and exp_line.find('л') < 0:
                    time_interval['years'] = 0
                else:
                    time_interval['years'] = int(exp_line[:3].replace(' ', ''))

                if exp_line.find('мес') < 0:
                    time_interval['months'] = 0
                else:
                    time_interval['months'] = int(exp_line[sep_months - 3:len(exp_line) - 3].replace(' ', ''))

            else:
                time_interval = {'years': 0, 'months': 0}

            month_end_number = self.months_numbers().get(month_end.lower())
            exp_dict['end'] = '01' + '-' + str(month_end_number) + '-' + str(year_end)
            exp_dict['timeinterval'] = time_interval
            if element.find('p', {'class': 'company-name'}):
                exp_dict['company'] = element.find('p', {'class': 'company-name'}).getText().replace('\t', '')\
                    .replace('\n', '').replace('\"', '')
            else:
                exp_dict['company'] = ''

            exp_dict['position'] = element.find('p', {'class': 'last-position-name'}).getText().replace('\t', '')\
                .replace('\n', '')
            if element.find('p', {'class': 'lh_20 p-res-exp'}):
                exp_dict['description'] = element.find('p', {'class': 'lh_20 p-res-exp'}).getText()\
                    .replace('\t', '').replace('\n', '')
            else:
                exp_dict['description'] = ''

        return exp_dict

    def get_skills(self, element, **kwargs):
        data = []
        return data

    def get_education_level(self, element, **kwargs):
        if not self._education_data:
            self._get_education_data(element)

        return self._education_data['level']

    def get_education(self, element, **kwargs):
        if not self._education_data:
            self._get_education_data(element)

        return self._education_data['data']

    def _get_education_data(self, element):

        data = []
        blocks = element.find_all('div', {'class': 'td2 pt12'})
        education_level = ''
        for block in blocks:
            record, level = self.get_education_record(block)
            if level:
                education_level = level.strip()
            if record:
                data.append(record)

        result = {'data': data, 'level': education_level}

        self._education_data = result

        return result

    @staticmethod
    def get_education_record(element):
        education_level = ''
        ed_dict = {}
        if element.find('span', {'class': 'edu-type-info'}):  # Потому что по мудацки названы классы
            if element.find('span', {'class': 'mt_5 lh_20 edu-type-info'}):
                ed_dict['final'] = element.find('span', {'class': 'mt_5 lh_20 edu-type-info'}).getText()\
                    .replace('\t', '').replace('\n', '')
            else:
                ed_dict['final'] = ''
            if element.find('span', {'class': 'edu-type-info'}):
                ed_dict['name'] = element.find('span', {'class': 'edu-type-info'}).getText()\
                    .replace('\t', '').replace('\n', '').replace('\"', '')
            else:
                ed_dict['name'] = ''

            ed_dict['organization'] = ''  # нет
            ed_dict['specialization'] = ''  # нет

            if element.previous.previous.previous == 'Основное образование':
                education_level = element.find('span', {'class': 'bold edu-type'}).getText().\
                    replace('\t', '').replace('\n', '').replace(',', '')

        return ed_dict, education_level

    def get_resume_link(self, element, **kwargs):

        cv_link = kwargs.get('cv_link')

        if cv_link:
            return cv_link.split('?')[0]
        else:
            return ''

    def get_site_id(self, element, **kwargs):

        return element.find('div', {'class': 'b-invite-line__info'}).getText()[12:20]
