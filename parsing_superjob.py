import requests
from bs4 import BeautifulSoup as bs
import json

URL = 'https://www.superjob.ru/resume/programmist-1s.html?sbmit=1&t%5B0%5D=4'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

def is_digit(string):
    if string.isdigit():
       return True
    else:
        try:
            float(string)
            return True
        except ValueError:
            return False

def Getresponse(url, headers):
    response = requests.get(url)
    response.headers = HEADERS
    return response


def get_id(form):
    lis_sort = form.split('-')
    list_len = len(lis_sort)
    _id = lis_sort[list_len - 1].replace('.html', '')
    return _id


def get_adress(form):
    return form.find_all('div', {'class': '_2g1F-'})[16].getText()


def get_gender(form):
    """пол не доступен в карточке вакансии"""
    return ''


def get_salary_and_valuta(form):
    split_list = form.find('span', {'class': '_3mfro PlM3e _2JVkc _2VHxz'}).getText().split(' ')
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

def get_age(form):
    age_str = form.find_all('div', {'class': '_2g1F-'})[15].getText()[:2]
    if is_digit(age_str):
        return int(age_str)
    else:
        return 0

def get_position(form):
    return form.find('h1', {'class': '_3mfro s1nFK _2JVkc _2VHxz _15msI'}).getText()

def get_about_me(form):
    return form.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace('\n','')

def get_category(form):
    return ''

def get_specialization(form):
    return form.find('div', {'class': '_3mfro _2VtGa _1hP6a _2JVkc _2VHxz _3LJqf _15msI'}).getText().replace('\n','')

def get_еmployment(form):
    return form.find('span', {'class': '_3mfro _3EQE7 _2JVkc _2VHxz'}).getText().replace('\n','')

def get_work_schedule(html_form):
    return '' # ВЫЯСНИТЬ КАК ОТДЕЛЯТЬ ОТ EMPLOYMENT

def get_seniority(form):
    return form.find('span', {'class': '_3mfro _9fXTd _2JVkc'}).getText().replace('\n','')

def get_experience(form):
    experience = []
    experience_array = form.find_all('div', {'class': '_9tygw'}) #НАЙТИ СПОСОБ ОТДЕЛИТЬ МЕСТА РАБОТЫ ОТ МЕСТ УЧЕБЫ
    for exp in experience_array:

        exp_dict = {}
        exp_dict['start'] = ''
        exp_dict['end'] = ''
        exp_dict['timeinterval'] = ''
        exp_dict['company'] = ''
        exp_dict['position'] = ''
        exp_dict['description'] = ''

        experience.append(exp_dict)

    return experience

def get_skills(form):
    return []

def get_education_level(form):
    return form.find_all('div', {'class': '_2g1F-'})[15].getText().split(',')[-1]

def get_education(form):
    return ''





def ParsingForms(shortlist):
    for form in shortlist:

        url_form = 'https://www.superjob.ru' + form.get('href')
        response_form = Getresponse(url_form, HEADERS)
        if response_form.status_code == 200:
            html_form = bs(response_form.text, 'lxml')
            cv_fields_data = {}



            _id = get_id(url_form)
            address = get_adress(html_form)
            gender = get_gender(html_form)
            salary_valuta = get_salary_and_valuta(html_form)
            salary = salary_valuta[0]
            valuta = salary_valuta[1]
            age = get_age(html_form)
            position = get_position(html_form)
            about_me = get_about_me(html_form)
            category = get_category(html_form)
            specialization = get_specialization(html_form)
            еmployment = get_еmployment(html_form)
            work_schedule = get_work_schedule(html_form)
            seniority = get_seniority(html_form)
            experience = get_experience(html_form)
            skills = get_skills(html_form)
            education_level = get_education_level(html_form)
            education = get_education(html_form)

            cv_fields_data['_id'] = _id
            cv_fields_data['address'] = address
            cv_fields_data['gender'] = gender
            cv_fields_data['salary'] = salary
            cv_fields_data['valuta'] = valuta
            cv_fields_data['age'] = age
            cv_fields_data['position'] = position
            cv_fields_data['about_me'] = about_me
            cv_fields_data['category'] = category
            cv_fields_data['specialization'] = specialization
            cv_fields_data['еmployment'] = еmployment
            cv_fields_data['work_schedule'] = work_schedule
            cv_fields_data['seniority'] = seniority
            cv_fields_data['experience'] = experience
            cv_fields_data['skills'] = skills
            cv_fields_data['education_level'] = education_level
            cv_fields_data['education'] = education

            _cv_fields.append(cv_fields_data)

            print(cv_fields_data)

            with open('FormHTML_Soperjob.html', 'w', encoding='UTF-8') as file_form:
                file_form.write(response_form.text)


def ParsingList():
    response = Getresponse(URL, HEADERS)

    if response.status_code == 200:
        html = bs(response.text, 'lxml')
        amount_text = html.find('span', {'class': '_3mfro k6vMC PlM3e _2JVkc _2VHxz'}).getText().split(" ")[1]
        shortlist = html.find_all('a', {'class': '_3dPok'})
        print(shortlist)
    if len(shortlist) > 0:
        with open('SoperjobHTML.html', 'w', encoding='UTF-8') as file_form:
            file_form.write(response.text)
        ParsingForms(shortlist)


# Вывод для визуального контроля
# for element in _cv_fields:
# print(element)

# Создание списков
_cv_fields = []
_comp_fields = []

ParsingList()

# Сохранение в json
# with open('superjob.json', 'w', encoding='utf-8') as f:
# json.dump(_cv_fields, f, ensure_ascii=False, indent=4)
