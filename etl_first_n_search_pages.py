import os
import random
import time

import bonobo
import bs4
import pymongo
import requests
from bonobo.config import use_context_processor, use
from bs4 import ResultSet
from html_text import html_text
from requests import Response
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='tmp/myapp.log',
                    filemode='w')


def with_opened_request_file(context):
    with context.get_service('fs').open(os.getenv('FILE_LOCATION'), 'r') as query_file:
        yield query_file


@use_context_processor(with_opened_request_file)
def construct_queries(file):
    for request in file.readlines():
        raw_query, pages = request.strip().split(",")
        query = raw_query.replace(" ", "+")

        logging.info(f'extract query: {query}')

        yield {'origin': raw_query,
               'url': f'https://google.com/search?q={query}&filter=0',
               'page_count': int(pages)}


def gather_page_links(query_dict):
    related_url_title_lst = []

    for i in range(query_dict['page_count']):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        }

        request_result: Response = requests.get(f"{query_dict['url']}&start={i * 10}", headers=headers)
        soup: bs4.BeautifulSoup = bs4.BeautifulSoup(request_result.text, 'html.parser')

        headings: ResultSet = soup.find_all('h3')
        for heading in headings:
            try:
                title, url = heading.text, heading.parent['href']
            except KeyError:
                logging.warning(f'got key error on heading: {heading}')
                continue
            url = url[url.find('http'):url.find('&')]

            related_url_title_lst.append((title, url))

        delays = [7, 4, 6, 2, 10, 19]
        delay = random.choice(delays)
        time.sleep(delay)

    return {**query_dict, **{'title_page_url_pairs': related_url_title_lst}}


def gather_pages_text_content(query_dict):
    pairs = query_dict['title_page_url_pairs']

    for index, pair in enumerate(pairs):
        url = pair[1]
        request_result = requests.get(url)
        text = html_text.extract_text(request_result.text)
        pairs[index] = list(pairs[index]) + [{'html': request_result.text, 'text_only': text}]

    query_dict["title_page_url_pairs"] = pairs

    return query_dict


@use('mongo_collection')
def save_to_db(*args, mongo_collection):
    mongo_collection.insert_many(list(args))


def get_graph():
    graph = bonobo.Graph()
    graph.add_chain(
        construct_queries,
        gather_page_links,
        gather_pages_text_content,
        save_to_db,
    )
    return graph


def get_services():
    client = pymongo.MongoClient(os.getenv('MONGO_IP'),
                                 port=int(os.getenv('MONGO_PORT')),
                                 username=os.getenv('MONGO_USER'),
                                 password=os.getenv('MONGO_PASSWORD'))
    db = client[os.getenv('MONGO_DB')]
    collection = db[os.getenv('MONGO_COLLECTION')]
    return {
        'fs': bonobo.open_fs(os.getenv('FILE_SYSTEM_ROOT')),
        'mongo_collection': collection
    }


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            graph=get_graph(),
            services=get_services()
        )
