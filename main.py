import requests
from bs4 import BeautifulSoup
import time
import pymysql
from pathlib import Path
from random import randint
import os


def getLastDbId(mysqlCred):
    mysqlConnection = pymysql.connect(mysqlCred['hostname'], mysqlCred['username'],
                                      mysqlCred['password'], mysqlCred['database'])
    try:
        with mysqlConnection.cursor() as cursor:
            sql = cursor.execute('select LAST_INSERT_ID() FROM `avito_items`')
        mysqlConnection.commit()
    finally:
        mysqlConnection.close()
    if sql is not None:
        return int(sql)
    else:
        return 0


def addItemToDb(itemDict, mysqlCred):
    mysqlConnection = pymysql.connect(mysqlCred['hostname'], mysqlCred['username'],
                                      mysqlCred['password'], mysqlCred['database'])
    try:
        with mysqlConnection.cursor() as cursor:
            sql = "INSERT INTO `avito_items` (`id`, `name`, `description`, `price_rub`, `price_zl`, `item_url`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,
                           (itemDict['itemSQLid'], itemDict['name'], itemDict['description'], itemDict['price_rub'],
                            itemDict['price_zl'], itemDict['item_page_url']))
            for image_path_iter in itemDict['pageImagesList']:
                sql = "INSERT INTO `img_urls` (`id`, `img_url`) VALUES (%s, %s)"
                cursor.execute(sql, (itemDict['itemSQLid'], image_path_iter))
        mysqlConnection.commit()
    finally:
        mysqlConnection.close()


def checkItemParsed(url, mysqlCred):
    mysqlConnection = pymysql.connect(mysqlCred['hostname'], mysqlCred['username'],
                                      mysqlCred['password'], mysqlCred['database'])
    try:
        with mysqlConnection.cursor() as cursor:
            sql = "SELECT `id` FROM `avito_items` WHERE `item_url`=%s"
            cursor.execute(sql, url)
            query_result = cursor.fetchone()
        mysqlConnection.commit()
    finally:
        mysqlConnection.close()
    if query_result is None:
        return False
    else:
        return True


mysqlCred = {
    'hostname': 'localhost',
    'username': 'phpmyadmin',
    'password': 'some_pass',
    'database': 'matrasspl'
}

zl_price_rub = 19.72

img_save_path = "parsed_images/"
nextPageDelay = 10  # Delay in seconds between pages load (prevent bot ban)
baseUrl = "https://www.avito.ru"  # /kaliningrad/mebel_i_interer/kreslo_1994922468

headers_dict = {
    1: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'referer': 'https://www.google.ru/search?newwindow=1&sxsrf=ALeKk00dRkyX49x3-2Am-sFHe3u7sHF-AQ%3A1603902328745&source=hp&ei=eJuZX5TZKtqGjLsPpYKYwAQ&q=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BA%D0%B0%D0%BB%D0%B8%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4&oq=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BA%D0%B0%D0%BB%D0%B8%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4&gs_lcp=CgZwc3ktYWIQAzICCAAyAggAMgIIADICCAAyAggAMgIIADICCAAyBggAEBYQHjIGCAAQFhAeMgYIABAWEB46BAgjECc6CAgAELEDEIMBOgUIABCxAzoICC4QsQMQgwE6BQguELEDOgIILlDTBViiH2DTIWgAcAB4AIABVogBqQqSAQIxOZgBAKABAaoBB2d3cy13aXo&sclient=psy-ab&ved=0ahUKEwiU9bnk2dfsAhVaA2MBHSUBBkgQ4dUDCAc&uact=5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'Pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin'
    },
    2: {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'referrer': 'https://www.google.com/search?q=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BF%D0%BE%D0%BB%D1%8C%D1%88%D0%B0&oq=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BF%D0%BE%D0%BB%D1%8C%D1%88%D0%B0&aqs=chrome..69i57j0i22i30l3.3343j0j7&sourceid=chrome&ie=UTF-8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'Pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin'
    },
    3: {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
        'referrer': 'https://www.google.com/search?q=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BF%D0%BE%D0%BB%D1%8C%D1%88%D0%B0&oq=%D0%BC%D0%B0%D1%82%D1%80%D0%B0%D1%81%D1%8B+%D0%BF%D0%BE%D0%BB%D1%8C%D1%88%D0%B0&aqs=chrome..69i57j0i22i30l3.3343j0j7&sourceid=chrome&ie=UTF-8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'Pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin'
    },
    4: {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
        'referrer': 'https://yandex.ru/search/?lr=22&text=%D0%B0%D1%82%D1%81%20shop%20%D0%BA%D0%B0%D0%BB%D0%B8%D0%BD'
                    '%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'Pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin'
    }
}

url = 'https://avito.ru/matras39'

# shutil.rmtree('parsed_images', ignore_errors=True, onerror=None)
Path("parsed_images").mkdir(parents=True, exist_ok=True)

itemSQLid = getLastDbId(mysqlCred)
awaitTimerOverflow = 0
html_doc = requests.get(url, headers=headers_dict[randint(1, len(headers_dict))])
soup = BeautifulSoup(html_doc.text, "lxml")
totalPages = soup.find('a', text='Последняя', attrs={'class': 'pagination-page'})
if totalPages is None:
    banFlag = True
    os.system('sshpass -p 17856opYth-O ssh reboot_user@192.168.53.1 \':execute {/system reboot;}\'')
    while banFlag:
        awaitTime = randint(700, 1800)
        # print('Banned, next try in ', awaitTime, ' seconds\n')
        print('Banned, next try in 60 seconds\n')
        time.sleep(60)
        html_doc = requests.get(url, headers=headers_dict[randint(1, len(headers_dict))])
        soup = BeautifulSoup(html_doc.text, "lxml")
        totalPages = soup.find('a', text='Последняя', attrs={'class': 'pagination-page'})
        if totalPages is not None:
            banFlag = False

totalPages = int(totalPages['href'].split('=')[1])

for currentPageNumber in range(totalPages):
    currentPageNumber += 1
    url = 'https://avito.ru/matras39?p=' + str(currentPageNumber)
    html_doc = requests.get(url, headers=headers_dict[randint(1, len(headers_dict))])
    soup = BeautifulSoup(html_doc.text, "lxml")
    for link in soup.findAll('a', {'class': 'item-slider'}):
        item_page_url = str(baseUrl + link['href'])
        if not checkItemParsed(item_page_url, mysqlCred):
            try:
                # if awaitTimerOverflow == 100:
                # time.sleep(300)
                # awaitTimerOverflow = 0
                print("Parsing page №: ", itemSQLid)
                print("\n")
                try:
                    item_page = requests.get(baseUrl + link['href'],
                                             headers=headers_dict[randint(1, len(headers_dict))])
                except:
                    banFlag = True
                    while banFlag:
                        try:
                            item_page = requests.get(baseUrl + link['href'],
                                                     headers=headers_dict[randint(1, len(headers_dict))])
                            if item_page.status_code == 200:
                                banFlag = False
                        except:
                            pass
                item_parse_object = BeautifulSoup(item_page.text, "lxml")
                name = item_parse_object.find("span", attrs={"class": "title-info-title-text"})
                if name is None:
                    banFlag = True
                    os.system('sshpass -p 17856opYth-O ssh reboot_user@192.168.53.1 \':execute {/system reboot;}\'')
                    while banFlag:
                        awaitTime = randint(700, 1800)
                        # print('Banned, next try in ', awaitTime, ' seconds\n')
                        print('Banned, next try in 60 seconds\n')
                        time.sleep(60)
                        try:
                            item_page = requests.get(baseUrl + link['href'],
                                                     headers=headers_dict[randint(1, len(headers_dict))])
                        except:
                            banFlag = True
                            while banFlag:
                                try:
                                    item_page = requests.get(baseUrl + link['href'],
                                                             headers=headers_dict[randint(1, len(headers_dict))])
                                    if item_page.status_code == 200:
                                        banFlag = False
                                except:
                                    pass
                        item_parse_object = BeautifulSoup(item_page.text, "lxml")
                        name = item_parse_object.find("span", attrs={"class": "title-info-title-text"})
                        if name is not None:
                            banFlag = False
                name = name.text
                description = item_parse_object.find("div", attrs={"class": "item-description-text"}).find("p").text
                price_rub = int(item_parse_object.find("span", attrs={"class": "js-item-price"}).text.replace(" ", ""))
                price_zl = int(price_rub) / zl_price_rub
                imgs = item_parse_object.findAll("div", attrs={"class": "gallery-extended-img-frame"})
                pageImagesList = []
                for img in imgs:
                    img_url = img.get("data-url")
                    img_filename = img_url.split("/")[-1] + ".jpeg"
                    img_path = img_save_path + img_filename
                    pageImagesList.append(img_path)
                    img_get_response = requests.get(img_url)
                    img_save_file = open(img_path, "wb")
                    img_save_file.write(img_get_response.content)
                    img_save_file.close()
                itemFields = {
                    'itemSQLid': itemSQLid,
                    'name': name,
                    'description': description,
                    'price_rub': price_rub,
                    'price_zl': price_zl,
                    'item_page_url': item_page_url,
                    'pageImagesList': pageImagesList
                }
                addItemToDb(itemFields, mysqlCred)
                itemSQLid += 1
                awaitTimerOverflow += 1
                time.sleep(randint(3, nextPageDelay))
            except KeyError:
                pass
        else:
            print('Already parsed page', item_page_url, ', skipping to next one...\n')
