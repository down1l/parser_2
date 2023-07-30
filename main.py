from httpx import AsyncClient
from lxml import etree
from lxml.etree import HTMLParser


import os
import gc
import asyncio
import json
from typing import Union
import re
from datetime import datetime
import time


URL = "https://лучшиелакомства.рф/"


class ParseException(Exception):
    pass


async def ParseMainPage(url: str) -> Union[list, ParseException]:

    async with AsyncClient() as client:
        response = await client.get(url)

    if response.status_code == 200:
        tree = etree.fromstring(response.text, HTMLParser())
        product_links = tree.xpath(
            '//div[@class="jet-woo-product-thumbnail"]/a/@href')

        return product_links

    raise ParseException(f"Неудачный запрос на главную страницу {url}")


async def ParseProductPage(url: str) -> Union[dict, str]:

    async with AsyncClient() as client:
        response = await client.get(url)

    if response.status_code == 200:
        tree = etree.fromstring(response.text, HTMLParser())

        # Имя
        name = tree.xpath('//h1[@class="product_title entry-title"]/text()')[0]
        # ID
        id_ = re.search(r'postid-(\d+)',
                        tree.xpath('/html/body/@class')[0]).group(1)
        # Артикль
        article = tree.xpath('//span[@class="sku"]/text()')[0]
        # Цена
        price = tree.xpath(
            '//span[@class="woocommerce-Price-amount amount"]/bdi/text()')[0].strip()
        # Ссылка на картику
        image = tree.xpath(
            '//div[@class="woocommerce-product-gallery__image"]/a/@href')[0]
        # Описание
        description = tree.xpath(
            '//div[@class="woocommerce-product-details__short-description"]/p/span/text()') + tree.xpath(
            '//div[@class="woocommerce-product-details__short-description"]/p/text()')

        # Удаляет пустые строки из списка
        description = [row.strip() for row in description if row.strip()]
        description = "\n ".join(description)

        # Вложенность категорий
        categories = tree.xpath(
            '//nav[@class="woocommerce-breadcrumb"]/a/text()')
        categories = "/".join(categories)

        return name, id_, article, price, image, description, categories

    else:
        raise ParseException(f"Неудачный запрос на страницу {url}")


def MakeRecord(data: list) -> dict:
    names = ["Имя", "ID", "Артикль", "Цена",
             "Ссылка на картику", "Описание", "Вложенность категорий"]
    return dict(zip(names, data))


def MakeJSONdump(data: list) -> None:
    date = str(datetime.now()).split(" ")[0]
    with open(f"results/{date}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Данные записаны в result/{date}.json")


async def main():
    while True:
        dump = []

        try:
            product_links = await ParseMainPage(URL)
        except ParseException as e:
            print(f"Ошибка: {e}")

        for link in product_links:
            try:
                data = await ParseProductPage(link)
                dump.append(MakeRecord(data))

            except ParseException as e:
                print(f"Ошибка: {e}")

        MakeJSONdump(dump)

        # Очистка оперативной памяти
        del (dump, data, product_links)
        gc.collect()

        time.sleep(5 * 24 * 60 * 60)


if __name__ == "__main__":
    asyncio.run(main())
