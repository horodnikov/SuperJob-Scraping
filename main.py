import requests
import random
import time
import pickle
import re
import pandas
import json
from bs4 import BeautifulSoup


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
                        vacancy_data['min_salary'] = None
                        vacancy_data['max_salary'] = None
                        vacancy_data['currency'] = None
                        vacancy_data['period'] = None
                        vacancy_data['description'] = match.group(0)
                    else:
                        vacancy_salary = "".join(
                            re.findall(r'[./,—a-zA-Zа-яА-Я0-9]+',
                                       salary_info.text))
                        match = re.fullmatch(r'(\d+)\D(\d+)\s*(\w+.)\D(\w+)',
                                             vacancy_salary)
                        if match:
                            vacancy_data['min_salary'] = match.group(1)
                            vacancy_data['max_salary'] = match.group(2)
                            vacancy_data['currency'] = match.group(3)
                            vacancy_data['period'] = match.group(4)
                            vacancy_data['description'] = None
                        else:
                            match = re.fullmatch(
                                r'(\D+)\s*(\d+)\s*(\w+.)\D(\w+)',
                                vacancy_salary)
                            if match:
                                if match.group(1) == 'от':
                                    vacancy_data['min_salary'] = match.group(2)
                                    vacancy_data['max_salary'] = None
                                    vacancy_data['currency'] = match.group(3)
                                    vacancy_data['period'] = match.group(4)
                                    vacancy_data['description'] = None

                                elif match.group(1) == 'до':
                                    vacancy_data['min_salary'] = None
                                    vacancy_data['max_salary'] = match.group(2)
                                    vacancy_data['currency'] = match.group(3)
                                    vacancy_data['period'] = match.group(4)
                                    vacancy_data['description'] = None
                            else:
                                match = re.fullmatch(r'(\d+)\s*(\w+.)\D(\w+)',
                                                     vacancy_salary)
                                if match:
                                    vacancy_data['min_salary'] = match.group(1)
                                    vacancy_data['max_salary'] = match.group(1)
                                    vacancy_data['currency'] = match.group(2)
                                    vacancy_data['period'] = match.group(3)
                                    vacancy_data['description'] = None
                    vacancies_summary.append(vacancy_data)

            is_last_page = soup.find('a', attrs={'class':
                'icMQ_ bs_sM _3ze9n l9LnJ f-test-button-dalshe f-test-link-Dalshe'})

            print(f'Parsing page: {page_counter} finished')
            print(f'Vacancies found {len(vacancies_summary)}')
            page_counter += 1
            time.sleep(1)
        return vacancies_summary

    @staticmethod
    def save_pickle(file_object, file_path):
        with open(file_path, 'wb') as file:
            pickle.dump(file_object, file)

    @staticmethod
    def load_pickle(file_path):
        with open(file_path, 'rb') as file:
            return pickle.load(file)


if __name__ == "__main__":

    url = "https://russia.superjob.ru/vacancy/search"

    agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
            "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"

    path = "hw_super_job.rsp"

    search_word = input('Type of keyword to search: ')

    hh_parser = SuperJobParser(start_url=url, sleep=1, key_word=search_word,
                               user_agent=agent, timeout=10)

    result = hh_parser.parse()
    df = pandas.DataFrame(result)

    with open("vacancies.json", "w") as json_file:
        json.dump(result, json_file, indent=2, ensure_ascii=False)
