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

from houzz.spiders import ProfilesSpider, APISpider


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

    def close_spider(self, spider: APISpider):
        stats = spider.stats
        finish_time = datetime.datetime.utcnow()

        mongo_log = self.logs_collection.find_one({'process_hash': spider.process_hash})
        spider.logger.debug(f"MONGO LOG: {mongo_log}")
        if mongo_log is None:
            log = {
                'process_hash': spider.process_hash,
                'start_datetime': stats.get_value('start_time'),
                'finish_datetime': finish_time,
                'total_spent_time': (finish_time - stats.get_value('start_time')).total_seconds(),
                'profiles_added': stats.get_value('profiles_added'),
                'profiles_total': stats.get_value('profiles_total'),
                'error_count': 0 if stats.get_value('log_count/ERROR') is None else stats.get_value('log_count/ERROR'),
                'retries_count': stats.get_value('retry_times', 0),
            }
            self.logs_collection.insert_one(log)
        else:
            if stats.get_value('log_count/ERROR') is None:
                err_count = 0
            else:
                err_count = stats.get_value('log_count/ERROR')
            log = {
                'finish_datetime': finish_time,
                'total_spent_time': (finish_time - stats.get_value('start_time')).total_seconds(),
                'profiles_added': mongo_log['profiles_added'] + stats.get_value('profiles_added'),
                'error_count': mongo_log['error_count'] + err_count,
                'retries_count': mongo_log['error_count'] + stats.get_value('retry_times', 0),
            }
            self.logs_collection.update_one({'process_hash': spider.process_hash}, {'$set': log})

        self.client.close()

    def process_item(self, item, spider: ProfilesSpider):
        self.profile_collection.update({'contact_name': item['contact_name']}, dict(item), True)
        spider.stats.set_value('profiles_added', spider.stats.get_value('profiles_added', 0) + 1)
        spider.logger.info(f'Profile item "{item["contact_name"]}" processed')
        return item
