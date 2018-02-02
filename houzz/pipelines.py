# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import pymongo
import scrapy.crawler
from pymongo.collection import Collection
from pymongo.database import Database

from houzz.spiders import ProfilesSpider


class HouzzPipeline(object):
    profile_collection_name = 'profiles'
    logs_collection_name = 'logs'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None
        self.profile_collection = None
        self.logs_collection = None

    @classmethod
    def from_crawler(cls, crawler: scrapy.crawler.Crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client: pymongo.MongoClient = pymongo.MongoClient(self.mongo_uri)
        self.db: Database = self.client[self.mongo_db]
        self.profile_collection: Collection = self.db[self.profile_collection_name]
        self.logs_collection: Collection = self.db[self.logs_collection_name]

    def close_spider(self, spider: ProfilesSpider):
        stats = spider.stats
        finish_time = datetime.datetime.utcnow()
        self.logs_collection.insert_one({
            'start_datetime': stats.get_value('start_time'),
            'finish_datetime': finish_time,
            'total_spent_time': (finish_time - stats.get_value('start_time')).total_seconds(),
            'profiles_added': stats.get_value('profiles_added'),
            'profiles_total': stats.get_value('profiles_total'),
            'error_count': 0 if stats.get_value('log_count/ERROR') is None else stats.get_value('log_count/ERROR'),
            'retries_count': stats.get_value('retry_times', 0),
        })
        self.client.close()

    def process_item(self, item, spider: ProfilesSpider):
        self.profile_collection.update({'contact_name': item['contact_name'],
                                        'phone_number': item['phone_number']}, dict(item), True)
        spider.stats.set_value('profiles_added', spider.stats.get_value('profiles_added', 0) + 1)
        spider.logger.info(f'Profile item "{item["contact_name"]}" processed')
        return item
