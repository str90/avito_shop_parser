import requests
from bs4 import BeautifulSoup
import time
import pymysql
import shutil
from pathlib import Path
from random import randint
import os

mysqlCred = {
    'hostname': 'localhost',
    'username': 'phpmyadmin',
    'password': 'some_pass',
    'database': 'matrasspl'
}


def getLastDbId(mysqlCred):
    mysqlConnection = pymysql.connect(mysqlCred['hostname'], mysqlCred['username'],
                                      mysqlCred['password'], mysqlCred['database'])
    try:
        with mysqlConnection.cursor() as cursor:
            sql = cursor.execute('select LAST_INSERT_ID() FROM `avito_items`')
        mysqlConnection.commit()
    finally:
        mysqlConnection.close()
    return int(sql)

print(getLastDbId(mysqlCred))