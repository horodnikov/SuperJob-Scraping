import requests
import random
import time
import pickle
import re
import pandas
import json
from pymongo import MongoClient
import numpy
from bs4 import BeautifulSoup
from config_local_mongo import host, port
from pprint import pprint


class SuperJobParser:
    def __init__(self, start_url, sleep, key_word, user_agent, proxies=None,
                 retry_number=1, timeout=None):
        self.start_url = start_url
        self.sleep = sleep
        self.retry_number = retry_number
        self.params = {'keywords': f'{key_word}'}
        self.headers = {'User-Agent': f'{user_agent}'}
        self.proxies = proxies
        self.timeout = timeout

    def _get(self, *args, **kwargs):
        for i in range(self.retry_number):
            try:
                response = requests.get(*args, **kwargs)

                if response.status_code == 200:
                    return response
                else:
                    raise Exception
            except requests.exceptions.Timeout:
                pass
            except requests.exceptions.ProxyError:
                pass
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(self.sleep + random.random())
        return None

    def run(self):
        return self._get(self.start_url, headers=self.headers,
                         params=self.params, proxies=self.proxies,
                         timeout=self.timeout)

    def parse(self):
        vacancies_summary = []
        is_last_page = True
        page_counter = 0
        while is_last_page:
            self.params['page'] = page_counter
            print(f'Parse page: {page_counter}')
            hh_response = self.run()
            parser = hh_response.text
            soup = BeautifulSoup(parser, 'html.parser')
            vacancies_block = soup.find_all('div', attrs={
                'class': 'f-test-search-result-item'})
            for block in vacancies_block:
                vacancy = (
                    block.find('div', attrs={'class': 'Fo44F QiY08 LvoDO'}))
                if vacancy:
                    vacancy_data = {}
                    vacancy_info = vacancy.find('div', attrs={
                        'class': 'jNMYr GPKTZ _1tH7S'})
                    vacancy_data['vacancy_link'] = \
                        ''.join(self.start_url.rsplit('/search/vacancy')) + \
                        vacancy_info.find('a')['href']
                    vacancy_data['vacancy_title'] = vacancy_info.find('a').text
                    company_block = vacancy.find('div', attrs={
                        'class': '_3_eyK _3P0J7 _9_FPy'})
                    company_info = company_block.find('span', attrs={'class':
                        '_3Fsn4 f-test-text-vacancy-item-company-name _1_OKi _3DjcL _1tCB5 _3fXVo _2iyjv'})
                    location_info = company_block.find('span', attrs={'class':
                        'f-test-text-company-item-location _1_OKi _3DjcL _1tCB5 _3fXVo'})
                    vacancy_location = location_info.text.split('•')
                    vacancy_data['vacancy_city'] = \
                        vacancy_location[1].split(',')[0].strip()

                    if company_info:
                        vacancy_data['vacancy_company'] = company_info.find(
                            'a').text
                    else:
                        vacancy_data['vacancy_company'] = None
                    salary_info = vacancy_info.find('span', attrs={
                        'class': '_1OuF_ _1qw9T f-test-text-company-item-salary'})
                    match = re.fullmatch(r'(\D+)', salary_info.text)
                    if match:
                        vacancy_data['description'] = match.group(0)
                    else:
                        vacancy_salary = "".join(
                            re.findall(r'[./,—a-zA-Zа-яА-Я0-9]+',
                                       salary_info.text))
                        match = re.fullmatch(r'(\d+)\D(\d+)\s*(\w+.)\D(\w+)',
                                             vacancy_salary)
                        if match:
                            vacancy_data['min_salary'] = int(match.group(1))
                            vacancy_data['max_salary'] = int(match.group(2))
                            vacancy_data['currency'] = match.group(3)
                            vacancy_data['period'] = match.group(4)
                        else:
                            match = re.fullmatch(
                                r'(\D+)\s*(\d+)\s*(\w+.)\D(\w+)',
                                vacancy_salary)
                            if match:
                                if match.group(1) == 'от':
                                    vacancy_data['min_salary'] = int(match.group(2))
                                    vacancy_data['currency'] = match.group(3)
                                    vacancy_data['period'] = match.group(4)
                                elif match.group(1) == 'до':
                                    vacancy_data['max_salary'] = int(match.group(2))
                                    vacancy_data['currency'] = match.group(3)
                                    vacancy_data['period'] = match.group(4)
                            else:
                                match = re.fullmatch(r'(\d+)\s*(\w+.)\D(\w+)',
                                                     vacancy_salary)
                                if match:
                                    vacancy_data['min_salary'] = int(match.group(1))
                                    vacancy_data['max_salary'] = int(match.group(1))
                                    vacancy_data['currency'] = match.group(2)
                                    vacancy_data['period'] = match.group(3)
                    vacancies_summary.append(vacancy_data)

            is_last_page = soup.find('a', attrs={'class':
                'icMQ_ bs_sM _3ze9n l9LnJ f-test-button-dalshe f-test-link-Dalshe'})

            print(f'Parsing page: {page_counter} finished')
            print(f'Vacancies found {len(vacancies_summary)}')
            page_counter += 1
            time.sleep(1)
        return vacancies_summary

    @staticmethod
    def save_pickle(parse_object, file_path):
        with open(file_path, 'wb') as file:
            pickle.dump(parse_object, file)

    @staticmethod
    def load_pickle(file_path):
        with open(file_path, 'rb') as file:
            return pickle.load(file)

    @staticmethod
    def save_to_csv(parse_object, file_path):
        frame = pandas.DataFrame.from_records(parse_object)
        frame.to_csv(path_or_buf=file_path, index=True)

    @staticmethod
    def save_to_mongo(parse_object, db_name, db_collection, db_host=None, db_port=None):
        with MongoClient(host=db_host, port=db_port) as client:
            db = client[db_name]
            db.get_collection(db_collection).insert_many(parse_object)

    @staticmethod
    def update_mongo(parse_object, db_name, db_collection, db_host=None, db_port=None):
        with MongoClient(host=db_host, port=db_port) as client:
            db = client[db_name]
            for vacancy in parse_object:
                db.get_collection(db_collection).update_one({
                    'vacancy_link': vacancy['vacancy_link']}, {'$set': vacancy},
                    upsert=True)

    @staticmethod
    def mongo_find(db_name, db_collection, db_host=None, db_port=None):
        choice = int(
            input('1. Find vacancies with salary more than limit.\n'
                  '2. Find vacancies without salary specified. '))

        with MongoClient(host=db_host, port=db_port) as client:
            db = client[db_name]
            if choice == 1:
                salary = int(input('Please input salary more than limit '))
                vacancies = db.get_collection(db_collection).find(
                    {'$or': [
                        {
                            'max_salary': {'$gte': salary}
                        },
                        {
                            'min_salary': {'$gte': salary},
                            'max_salary': numpy.nan
                        }
                    ]})
            elif choice == 2:
                vacancies = db.get_collection('super_job').find(
                    {
                        'min_salary': numpy.nan,
                        'max_salary': numpy.nan
                    })
            else:
                'Please input correct number'

            for vacancy in vacancies:
                pprint(vacancy)


if __name__ == "__main__":

    url = "https://russia.superjob.ru/vacancy/search"

    agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
            "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"

    path = "hw_super_job.rsp"

    mongo_db = "vacancies"
    collection_name = 'super_job'

    search_word = input('Type of keyword to search: ')

    hh_parser = SuperJobParser(start_url=url, sleep=1, key_word=search_word,
                               user_agent=agent, timeout=10)

    result = hh_parser.parse()
    df = pandas.DataFrame(result)
    df = df.where(pandas.notnull(df), None)
    data = df.to_dict(orient='records')

    hh_parser.update_mongo(data, mongo_db, collection_name, host, port)
    hh_parser.mongo_find(mongo_db, collection_name, host, port)

    with open("super_job_data.json", "w") as json_file:
        json.dump(result, json_file, indent=2, ensure_ascii=False)
