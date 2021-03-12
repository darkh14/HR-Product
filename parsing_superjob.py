import requests
from bs4 import BeautifulSoup as bs
import json

URL = 'https://www.superjob.ru/resume/programmist-1s.html?sbmit=1&t%5B0%5D=4'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

def Getresponse(url, headers):
    response = requests.get(url)
    response.headers = HEADERS
    return response

def get_id(form):
    lis_sort = form.split('-')
    list_len = len(lis_sort)
    _id = lis_sort[list_len-1].replace('.html', '')
    return _id

def ParsingForms(shortlist):
    for form in shortlist:

        url_form = 'https://www.superjob.ru'+form.get('href')
        response_form = Getresponse(url_form, HEADERS)
        if response_form.status_code == 200:
            html_form = bs(response_form.text, 'lxml')
            cv_fields_data = {}

            _id = get_id(url_form)
            address = get_adress(response_form)


            cv_fields_data['_id'] = _id
            cv_fields_data['address'] = address
            # cv_fields_data['gender'] = gender
            # cv_fields_data['salary'] = salary
            # cv_fields_data['valuta'] = valuta
            # cv_fields_data['age'] = age
            #  cv_fields_data['position'] = position
            # cv_fields_data['about_me'] = about_me
            # cv_fields_data['category'] = category
            # cv_fields_data['specialization'] = specialization
            # cv_fields_data['еmployment'] = еmployment
            # cv_fields_data['work_schedule'] = work_schedule
            # cv_fields_data['seniority'] = seniority
            # cv_fields_data['experience'] = experience
            # cv_fields_data['skills'] = skills
            # cv_fields_data['education_level'] = education_level
            # cv_fields_data['education'] = education

            _cv_fields.append(cv_fields_data)

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

#Вывод для визуального контроля
#for element in _cv_fields:
    #print(element)

#Создание списков
_cv_fields = []
_comp_fields = []

ParsingList()

#Сохранение в json
#with open('superjob.json', 'w', encoding='utf-8') as f:
    #json.dump(_cv_fields, f, ensure_ascii=False, indent=4)

