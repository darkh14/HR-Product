import requests
from bs4 import BeautifulSoup as bs
from .base_parser import BaseParser

class SuperJobParser(BaseParser):

    name = 'SuperJob'
    url = 'superjob.ru/resume/programmist-1s.html?sbmit=1&t%5B0%5D=4'
    enable = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.params = []

        self.parsing_filter = kwargs.get('filter') or {'text': 'Программист 1с'}

        self._url_params_to_add = {}

        self._set_url_params()

        self.kwargs.update(kwargs)

    def _parse_with_parameters(self, url='', request_params={}, count=0, limit=0):
        params = request_params.copy() or []
        first_page = self._get_response(url, headers=self.headers, params=params)

        if first_page != None:
            eterations = int(first_page.find_all('span', {'class': '_1BOkc'})[36].getText())
            counter = 1
            eterations = 5
            while counter <= eterations:
                if counter == 1:
                    parsing_page = first_page
                    shortlist = parsing_page.find_all('a', {'class': '_3dPok'})
                else:
                    url_page = url + '&page=' + str(counter)
                    next_page = self._get_response(url_page, headers=self.headers, params=params)
                    shortlist = next_page.find_all('a', {'class': '_3dPok'})

                if len(shortlist) > 0:
                    for form in shortlist:
                        url_form = 'https://www.superjob.ru' + form.get('href')
                        response_form = self._get_response(url_form, headers=self.headers, params=params)
                        if response_form:
                            html_form = bs(response_form.text, 'lxml')
                            print(url_form)
                            cv_data = self.get_cv_data(html_form, cv_link=url_form)
                            self.write_cv_data(cv_data)

    def get_monthNumber(self, month):
        MonthsList = {}

        MonthsList['январь'] = '01'
        MonthsList['февраль'] = '02'
        MonthsList['март'] = '03'
        MonthsList['апрель'] = '04'
        MonthsList['май'] = '05'
        MonthsList['июнь'] = '06'
        MonthsList['июль'] = '07'
        MonthsList['август'] = '08'
        MonthsList['сентябрь'] = '09'
        MonthsList['октябрь'] = '10'
        MonthsList['ноябрь'] = '11'
        MonthsList['декабрь'] = '12'

        MonthNumber = ''

        for key in MonthsList:
            if key == month.lower():
                MonthNumber =  MonthsList[month]


        return MonthNumber

    def is_digit(self, string):
        if string.isdigit():
           return True
        else:
            try:
                float(string)
                return True
            except ValueError:
                return False

    def Getresponse(self, url, headers):
        response = requests.get(url)
        response.headers = HEADERS
        return response

    def get_site_id(self, element, **kwargs):
        lis_sort = element.split('-')
        list_len = len(lis_sort)
        _id = lis_sort[list_len - 1].replace('.html', '')
        return _id

    def get_address(self, element, **kwargs):
        return element.find_all('div', {'class': '_2g1F-'})[16].getText().replace(' ',' ')

    def get_gender(self, element, **kwargs):
        """пол не доступен в карточке вакансии"""
        return ''

    def get_salary_and_valuta(self, element, **kwargs):
        split_list = element.find('span', {'class': '_3mfro PlM3e _2JVkc _2VHxz'}).getText().split(' ')
        len_list = len(split_list)
        if len_list < 2:
            return [0, '']
        else:
            i = 0
            salary = ''
            while i < len_list-1:
                salary += split_list[i]
                i += 1
            return [int(salary), split_list[-1]]

    def get_salary(self, element, **kwargs):
        split_list = element.find('span', {'class': '_3mfro PlM3e _2JVkc _2VHxz'}).getText().split(' ')
        len_list = len(split_list)
        if len_list < 2:
            return [0, '']
        else:
            i = 0
            salary = ''
            while i < len_list - 1:
                salary += split_list[i]
                i += 1
            return [int(salary), split_list[-1]][0]

    def get_valuta(self, element, **kwargs):
        split_list = element.find('span', {'class': '_3mfro PlM3e _2JVkc _2VHxz'}).getText().split(' ')
        len_list = len(split_list)
        if len_list < 2:
            return [0, '']
        else:
            i = 0
            salary = ''
            while i < len_list - 1:
                salary += split_list[i]
                i += 1
            return [int(salary), split_list[-1]][1]

    def get_age(self, element, **kwargs):
        age_str = element.find_all('div', {'class': '_2g1F-'})[15].getText()[:2]
        if self.is_digit(age_str):
            return int(age_str)
        else:
            return 0

    def get_position(self, element, **kwargs):
        return element.find('h1', {'class': '_3mfro s1nFK _2JVkc _2VHxz _15msI'}).getText()

    def get_about_me(self, element, **kwargs):
        if element.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}):
            return element.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace('\n','').replace("\\",'')
        else:
            return ''

    def get_category(self, element, **kwargs):
        return ''

    def get_specialization(self, element, **kwargs):
        if element.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}):
            return [element.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace('\n','').replace(' ',' ').strip().replace("\\",'')]
        else:
            return ''

    def get_employment(self, element, **kwargs):
        return element.find('span', {'class': '_3mfro _3EQE7 _2JVkc _2VHxz'}).getText().replace('\n','').replace(' ',' ')

    def get_work_schedule(self, element, **kwargs):
        return '' # ВЫЯСНИТЬ КАК ОТДЕЛЯТЬ ОТ EMPLOYMENT

    def get_seniority(self, element, **kwargs):
        if element.find('span', {'class': '_3mfro _9fXTd _2JVkc'}):
            return element.find('span', {'class': '_3mfro _9fXTd _2JVkc'}).getText().replace('\n','').replace(' ',' ')
        else:
            return ''

    def get_experience(self, element, **kwargs):
        experience = []
        experience_array = element.find_all('div', {'class': '_9tygw'})
        for exp in experience_array:
            if len(exp.find_all('div', {'class': '_2g1F-'})) > 8:
                if exp.find_all('div', {'class': '_2g1F-'})[7]:
                    if exp.find_all('div', {'class': '_2g1F-'})[7].getText() == 'Обязанности:':

                        exp_dict = {}

                        date_list = exp.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[0].getText().split('–')
                        date_start_list = date_list[0].split(' ')
                        date_end_list = date_list[1].split(' ')
                        exp_dict['start'] = '01-' + self.get_monthNumber(date_start_list[0].lower())+'-'+date_start_list[1]
                        if  date_list[0] == ' работает сейчас':
                            exp_dict['end'] = 'работает сейчас'
                        else:
                            exp_dict['end'] = '01-' + self.get_monthNumber(date_end_list[0].lower().replace(' ', ''))+'-'+ date_end_list[1]

                        interval_list = exp.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[1].getText().split('и')
                        timeinterval = {}
                        if len(interval_list) == 0:
                            timeinterval['years'] = ''
                            timeinterval['months'] = ''
                        elif len(interval_list) == 1:
                            if interval_list[0].find('мес')<0:
                                timeinterval['years'] = interval_list[0][:2].replace(' ','')
                                timeinterval['months'] = ''
                            else:
                                timeinterval['years'] = ''
                                timeinterval['months'] = interval_list[0][:2].replace(' ', '')
                        elif len(interval_list) == 2:
                            timeinterval['years'] = interval_list[0][:2].replace(' ', '')
                            timeinterval['months'] = interval_list[1][:2].replace(' ', '')
                        exp_dict['timeinterval'] = timeinterval

                        exp_dict['company'] = exp.find('div', {'class': '_3mfro _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace(' ', ' ')
                        exp_dict['position'] = exp.find('h3', {'class': '_3mfro _1ZlLP _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace(' ', ' ')
                        exp_dict['description'] = exp.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace(' ', ' ')

                        experience.append(exp_dict)

        return experience

    def get_skills(self, element, **kwargs):
        return []

    def get_education_level(self, element, **kwargs):
        info_list = element.find_all('div', {'class': '_2g1F-'})[15].getText().split(',')
        if len(info_list)>2:
            ed_level = info_list[1].replace(' ',' ').strip()
        else:
            ed_level = ''
        return ed_level

    def get_education(self, element, **kwargs):
        education = []
        education_array = element.find_all('div', {'class': '_9tygw'})
        for ed in education_array:
            if len(ed.find_all('div', {'class': '_2g1F-'})) == 5:
               if ed.find_all('div', {'class': '_2g1F-'})[2].getText().find('Факультет:')>-1:
                    ed_dict = {}
                    param_list = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})
                    ed_dict['name']= ed.find('h3', {'class': '_3mfro _1ZlLP _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace(' ', ' ')
                    if len(param_list) == 5:
                        ed_dict['final'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[2].getText()
                        ed_dict['organization'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[3].getText().replace(' ', ' ')
                        ed_dict['specialization'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[4].getText().replace(' ', ' ').replace('Специальность:' , ' ')
                    elif len(param_list) == 4:
                        ed_dict['final'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[1].getText()
                        ed_dict['organization'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[2].getText().replace(' ',' ')
                        ed_dict['specialization'] = ed.find_all('div', {'class': '_3mfro _9fXTd _2JVkc _2VHxz _3LJqf _15msI'})[3].getText().replace(' ',' ').replace('Специальность:', '').strip()

                    education.append(ed_dict)
        return education


