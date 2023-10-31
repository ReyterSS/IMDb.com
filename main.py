import csv
import re
from aiocsv import AsyncWriter
from bs4 import BeautifulSoup
import asyncio, threading
import aiohttp
import time
import fake_useragent
import random
import aiofiles
#import AsyncWriter

useragent1 = fake_useragent.UserAgent().random
useragent = {'User-Agent': useragent1}
# entry = f'{http://{login}:{password}@{ip}:{port}}'
# proxy_auth = [
#     {"login": "Reyter234-zone-resi", "password": "scraping1", "ip": "pr.pyproxy.com", "port": "16666"}
# ]
# proxy = {
#     'http': entry,
#     'https': entry
# }

HOST = 'https://www.imdb.com'

data = {
    'authority': 'www.imdb.com',
    'method': 'GET',
    'path': '/search/title/?title_type=feature&sort=num_votes,desc',
    'scheme': 'https',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'max-age=0',
    'Cookie': 'session-id-time=2082787201l; session-id=140-5655254-2562420; ubid-main=132-0032670-9859934; ad-oo=0; '
              'uu=eyJpZCI6InV1ZDgwMDVmYWRhM2RhNGNkYTkwYjQiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfX0=; '
              'session-token=yNPan8SmkjcTPHbTzALle5RoBP5L8sVPJNOv5vOzJQTJJR16wjuKYg81CEjFcQMMowjUxGvHhZWGfwPAVZRq8q/'
              'TDxUml2bLF2fE/HMA3vbgKleD4lg7dEUckwh5fuXFjyIbNt3yOLCpM0IwSSZWeygxOOiys3/XAjdp3zcoDwSTbRqAnDSroUg+'
              'e3JB3TCIdVjXjUh2z/6mgao/xGk5HPRRA22lqRrFruyen5pHkFNHHRaDRBfuRW+5NT3upkriyn+Q4Zp0kSWNeZz/gwtlto/'
              'xy1HNocfBdQ2JKEjs8Fji0RFLIltDpFlx224rKj7iTDqiZzIWVHdXcxOUh7M4vqJRDLn84esF; csm-hit=adb:adblk_yes&t:'
              '1694264276563&tb:Z1JRTCYWR2CVWHG3J1HF+b-Z1JRTCYWR2CVWHG3J1HF|1694264276563',
    'Sec-Ch-Ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': f'{useragent1}'
}

async def get_page_list():
    async with aiohttp.ClientSession() as session:
        url = 'https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc'
        r = await session.get(url, headers=data)
        soup = BeautifulSoup(await r.text(), 'lxml')
        main_block = soup.find('div', class_='desc').find('span').text
        num_pages = re.findall(r'of(.*)titles', main_block, re.DOTALL)[0].replace(',', '').strip()
        pages_list = []
        for i in range(1, int(num_pages), 50):
            if i < 10001:
                page_template = f'https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc&start={i}' \
                                f'&ref_=adv_nxt'
                pages_list.append(page_template)
        return pages_list[:10]

async def get_date(pages_list):
    async with aiohttp.ClientSession() as session:
        r = await session.get(pages_list, headers=data)
        soup = BeautifulSoup(await r.text(), 'lxml')
        main_block = soup.find('div', class_='article')
        try:
            film_blocks = main_block.findAll('div', class_='lister-item-content')  # [35:40]
        except:
            film_blocks = ''
        films_url = []
        for num, i in enumerate(film_blocks, start=1):
            film_url0 = i.find(class_='lister-item-header')
            film_url2 = film_url0.find('a').get('href')
            HEADERS_EACH = {
                'authority': 'www.imdb.com',
                'method': 'GET',
                'path': f'{film_url2}',
                'scheme': 'https',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                          '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Cookie': 'session-id-time=2082787201l; session-id=140-5655254-2562420; ubid-main=132-0032670-'
                          '9859934; uu=eyJpZCI6InV1ZDgwMDVmYWRhM2RhNGNkYTkwYjQiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1'
                          'ZGVfYWR1bHQiOmZhbHNlfX0=; _gcl_au=1.1.1169992373.1694274606; ad-oo=0; session-token='
                          'rwvRZ1/1H5VPGpI9rBtM82K27/qO7mpGgl66TV3ivG2gdBxTp03J+pjtg9LCfKHV0j3WP0KJ7x7tvwKJxxz3oSD'
                          'c8sS0oN22c5KbSnDq4BjJeFRRYjVALpCS2UaGKPtmXfcjd00geDxcS4JYxN2R8tdjlDl8Dk+AY2hIgeYbrMJM9O'
                          'g1UoIckw3Cy1JjrCr1hUyz0ws8A6vw1W4Yjtt2tCqQy/b+PFsaSLORUuuqvnTvj33PwaaKJkERcVdCCaeRQk2GZ'
                          'jZWZ1dlGUdl33kHXqDyYe5ppW4MOpazxThHH/02y+Z9zTjs8+3BSlzWnWd3elNWUBJw9bL2g0jwju45GjLkALDPE'
                          'lzD; _uetsid=8c8750204f2811eeba6105726428bcfc; _uetvid=8c8792004f2811ee8d1c6d53f87e8b15;'
                          ' csm-hit=adb:adblk_no&t:1694430192251&tb:s-S21R7CBWDR31Y37CH888|1694430192251',
                'Referer': 'https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc',
                'Sec-Ch-Ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': f'{useragent1}'
            }
            film_num_chace = film_url2.split('/')[2]
            film_url = HOST + film_url0.find('a').get('href')
            films_url.append(film_url)
            # num_list = enumerate(films_url, start=1)
            r2 = await session.get(film_url, headers=HEADERS_EACH)
            soup2 = BeautifulSoup(await r2.text(), 'lxml')
            top_block = soup2.find(class_='ipc-page-section ipc-page-section--baseAlt ipc-page-section--tp-none ipc'
                                          '-page-section--bp-xs sc-e226b0e3-2 fxgTov')

            try:
                title0 = top_block.find(class_='sc-afe43def-3 EpHJp').text
                title = re.findall(r':(.*)', title0, re.DOTALL)[0].strip()
            except:
                title = ' '
            try:
                main_block = soup2.find(
                    class_='sc-9178d6fe-1 kFxVZc ipc-page-grid__item ipc-page-grid__item--span-2')
            except:
                main_block = ''
            try:
                details_block = main_block.find(attrs={'data-testid': 'Details'})
            except:
                details_block = ''
            try:
                realese_date0 = details_block.find(
                    class_='ipc-metadata-list-item__list-content-item ipc-metadata-list-'
                           'item__list-content-item--link').text
                realese_date = realese_date0.split('(')[0]
            except:
                realese_date = ' '
            try:
                rating0 = top_block.find(attrs={'data-testid': 'hero-rating-bar__aggregate-rating'}).text
                rating1 = rating0.replace('IMDb RATING', ' ').strip()
                rating = rating1.split('/')[0] + '/10', rating1.split('10')[1]
            except:
                rating = ' '
            pro_url = top_block.find(class_='sc-2662c89-0 gYVhmf pro-upsell pro-upsell--dark celwidget').get('href')
            r4 = await session.get(pro_url, headers=HEADERS_EACH)
            soup_pro = BeautifulSoup(await r4.text(), 'lxml')
            try:
                genre = soup_pro.find('div', id='title_heading').find('span', id='genres').text
            except:
                genre = ' '
            try:
                producer = soup_pro.find('div', id='producer_summary').find(
                    class_='a-fixed-left-grid-col a-col-right').find('span').text.strip()
            except:
                producer = 'Error'
            try:
                top_cast1 = soup2.find('div',
                                       class_='ipc-shoveler ipc-shoveler--base ipc-shoveler--page0 title-cast_'
                                              '_grid').find_all(class_='sc-bfec09a1-5 kUzsHJ')
                top_cast = []
                for i in top_cast1:
                    true = i.find(class_='sc-bfec09a1-1 fUguci').text
                    film = i.find(class_='sc-bfec09a1-4 llsTve').text
                    top_cast.append(true)
                    top_cast.append(film)
            except:
                top_cast = ' '
            try:
                writer0 = top_block.find_all(attrs={'data-testid': 'title-pc-principal-credit'})[1].find_all(class_=
                                                                                                             'ipc-inline-list__item')
                writer = []
                for i in writer0:
                    writer.append(i.text)
            except:
                writer = ' '
            try:
                runtime = soup2.find(attrs={'data-testid': 'title-techspec_runtime'}).find('div').text
            except:
                runtime = ' '
            try:
                language = details_block.find(attrs={'data-testid': 'title-details-languages'}).find(class_=
                                                                                                     'ipc-metadata-list-item__label').find_next_sibling().get_text(
                    separator=', ')
            except:
                language = ' '
            try:
                director0 = top_block.find_all(attrs={'data-testid': 'title-pc-principal-credit'})[0].get_text()
                director = re.findall(r'Director(.*)', director0, re.DOTALL)
            except:
                director = ' '
            try:
                country0 = details_block.find(attrs={'data-testid': 'title-details-origin'}).get_text(', ')  # [1:]
                country = re.findall(r'origin, (.*)', country0, re.DOTALL)
            except:
                country = ' '
            try:
                budget0 = soup2.find(attrs={'data-testid': 'title-boxoffice-budget'}).text
                budget = re.findall(r'Budget(.*)', budget0, re.DOTALL)
            except:
                budget = ' '
            print(num, title, realese_date, rating, genre, producer, top_cast, writer, runtime, language, director,
                  country, budget)
            data_film = {
                # "Номер фыльма": num,
                'Назва фільму': title,
                "Дата релізу":  realese_date,
                "Рейтинг": rating,
                "Жанр":genre,
                "Продюсер": producer,
                "Головні ролі": top_cast,
                "Письменник": writer,
                "Тривалість": runtime,
                "Мова": language,
                "Директор": director,
                "Країна": country,
                "Бюджет": budget,
                "Посилання": film_url
            }
            file_name = 'Films.csv'
            async with aiofiles.open(file_name, 'a', encoding='utf-8') as f:
                w = csv.DictWriter(f, data_film.keys())
                if f.tell() == 0:
                    w.writeheader()
                await w.writerow(data_film)

async def main():
    start = time.perf_counter()
    pages_list = await get_page_list()
    tasks = []
    for i in pages_list:
        print(i)
        tasks.append(asyncio.create_task(get_date(i)))
    await asyncio.gather(*tasks)
    fininsh = time.perf_counter()
    print(f'Выполнение заняло {fininsh-start: .2f} секунд')

if __name__ == '__main__':
    asyncio.run(main())
