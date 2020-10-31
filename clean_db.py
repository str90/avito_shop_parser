import pymysql
import shutil
from pathlib import Path

shutil.rmtree('parsed_images', ignore_errors=True, onerror=None)
Path("parsed_images").mkdir(parents=True, exist_ok=True)

mysqlConnection = pymysql.connect(host='localhost',
                                  user='phpmyadmin',
                                  password='some_pass',
                                  db='matrasspl',
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)

try:
    with mysqlConnection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE img_urls")
        cursor.execute("ALTER TABLE matrasspl.img_urls DROP FOREIGN KEY img_urls_ibfk_1")
        cursor.execute("TRUNCATE TABLE avito_items")
        cursor.execute("ALTER TABLE `img_urls` ADD FOREIGN KEY (`id`) REFERENCES `avito_items`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;")
    mysqlConnection.commit()

finally:
    mysqlConnection.close()
