# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import phonenumbers
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join


def strip(text):
    """
    Strip given text

    :param text:
    :return:
    """
    return text.strip()


def inline(text):
    """
    Replace all \t and \n symbols on ' ' to make text inline
    :param text:
    :return:
    """
    return text.replace('\t', ' ').replace('\n', '. ')


def format_phone(phone: str, country_code: str) -> str:
    """
    Format given phone number in E164

    :param phone: unformatted phone number
    :param country_code: country code of phone number
    :return: formatted phone number
    """
    locale = phonenumbers.parse(phone, country_code.upper())
    return phonenumbers.format_number(locale, phonenumbers.PhoneNumberFormat.E164)


class ProfileLoader(ItemLoader):
    _to_int = lambda x: int(x)
    _to_float = lambda x: float(x)

    default_output_processor = TakeFirst()

    coordinates_out = MapCompose()

    service_cost_in = MapCompose(inline)
    service_cost_out = Join()

    reviews_count_in = MapCompose(_to_int)
    projects_done_count_in = MapCompose(_to_int)
    pro_rating_in = MapCompose(_to_float)

    company_name_in = MapCompose(strip)


class Profile(scrapy.Item):
    activity_area = scrapy.Field()
    contact_name = scrapy.Field()

    address = scrapy.Field()
    coordinates = scrapy.Field()

    company_name = scrapy.Field()
    service_cost = scrapy.Field()

    reviews_count = scrapy.Field()
    projects_done_count = scrapy.Field()

    website = scrapy.Field()
    email = scrapy.Field()
    profile_url = scrapy.Field()

    phone_number = scrapy.Field()
    pro_rating = scrapy.Field()


class Address(scrapy.Item):
    prefecture = scrapy.Field(
        output_processor=TakeFirst()
    )  #
    city = scrapy.Field(
        output_processor=TakeFirst()
    )  #
    street = scrapy.Field(
        output_processor=Join(' ')
    )  #
    postal = scrapy.Field(
        output_processor=TakeFirst()
    )  #
