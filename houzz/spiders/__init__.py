# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import phonenumbers
import scrapy
from geopy import Nominatim
from geopy.exc import GeocoderTimedOut
from scrapy.loader import ItemLoader
from scrapy.statscollectors import MemoryStatsCollector

from houzz.items import Profile, Address, ProfileLoader
from houzz.settings import PROXY_ADDR


class ProfilesSpider(scrapy.Spider):
    name = 'profiles'
    start_urls = [
        'https://www.houzz.jp/professionals'
    ]

    def __init__(self, stats: MemoryStatsCollector, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.extracted = 0
        self.stats = stats
        self.geo_coder = None  # realized lazy connection to geo coder

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'proxy': PROXY_ADDR})

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(crawler.stats, *args, **kwargs)
        spider._set_crawler(crawler)
        return spider

    def parse(self, response: scrapy.http.TextResponse):
        """
        The function parses recursively the list of professionals while amount of extracted profiles wasn't exceed
        MAX_COUNT value.
        """
        if not self.stats.get_value('profiles_total', None):
            total = int(''.join(response.css(".main-title::text").re(r"\d+")))
            self.stats.set_value('profiles_total', total)
        for href in response.css('a.pro-title::attr(href)'):
            if self.extracted >= self.settings.get('MAX_COUNT'):
                return
            self.extracted += 1
            yield response.follow(href, callback=self.parse_profile, meta={'url': href, 'proxy': PROXY_ADDR})

        for href in response.css('a.navigation-button'):
            yield response.follow(href, callback=self.parse, meta={'proxy': PROXY_ADDR})

    def parse_profile(self, response: scrapy.http.TextResponse):
        """
        Parses the profile of each professional and redirect to professional projects page to grab their amount

        """
        postal = response.css(".pro-info-horizontal-list .info-list-text [itemprop=postalCode]::text").extract_first()
        coordinates, country_code = self.geolocate(postal)

        l = ProfileLoader(item=Profile(), response=response)
        l.add_value('contact_name',
                    response.css(".pro-info-horizontal-list .info-list-text")[1].css(":not(b)::text").re(r'[\w ]+'))
        l.add_css('activity_area', ".pro-info-horizontal-list .info-list-text [itemprop=child] [itemprop=title]::text")

        unformatted_phone = response.css(".pro-contact-methods .pro-contact-text::text").extract_first()
        l.add_value('phone_number', self.format_phone(unformatted_phone, country_code))

        l.add_css('website', ".pro-contact-methods .proWebsiteLink::attr(href)")

        al = ItemLoader(item=Address(), response=response)
        al.add_value('postal', postal)
        al.add_css('prefecture', ".pro-info-horizontal-list .info-list-text [itemprop=addressRegion]::text")
        al.add_css('street', ".pro-info-horizontal-list .info-list-text [itemprop=streetAddress]::text")
        al.add_css('city', ".pro-info-horizontal-list .info-list-text [itemprop=addressLocality] a::text")

        address = al.load_item()
        l.add_value('address', dict(address))

        l.add_value('coordinates', coordinates)

        l.add_css('company_name', "a.profile-full-name::text")

        try:
            price: scrapy.Selector = response.css(".pro-info-horizontal-list .info-list-text")[4]
            l.add_value('service_cost', price.css("::text").extract()[2:])
        except IndexError:
            pass
        l.add_value('profile_url', response.url)

        l.add_css('pro_rating', ".profile-title .pro-rating [itemprop=ratingValue]::attr(content)")
        l.add_css('reviews_count', ".profile-title .pro-rating [itemprop=reviewCount]::text")

        projects_url = response.css("a.sidebar-item-label[compid=projects_tab]::attr(href)")[0]

        yield response.follow(projects_url, callback=self.parse_projects_count, meta={'profile_loader': l,
                                                                                      'proxy': PROXY_ADDR})

    def parse_projects_count(self, response: scrapy.http.TextResponse):
        profile_loader: ProfileLoader = response.meta['profile_loader']
        try:
            profile_loader.add_value('projects_done_count', response.css("#projectsBody .header-1::text").re(r'\d+'))
        except IndexError:
            pass
        yield profile_loader.load_item()

    def geolocate(self, postal: str, default_code: str='JP'):
        """
        Processor to transform postal into coordinates on the Globe.

        *NOTE*: Be careful because the function is depending on chosen geo driver very much.
        Now it is set for use with Open Streets Map ``Nominatim`` driver

        :param postal: postal code
        :param default_code: default country code to use
        :return: geo longitude, geo.latitude, country_code
        """
        if not self.geo_coder:
            self.geo_coder = Nominatim(country_bias=self.settings.get('GEO_BIAS'))
        try:
            geo = self.geo_coder.geocode(postal, addressdetails=True)
        except GeocoderTimedOut as error:
            self.logger.error("Geocoder timed out, couldn't identify location")
            return None, default_code
        if not geo:
            return None, default_code
        return (geo.longitude, geo.latitude), geo.raw['address']['country_code']

    def format_phone(self, phone: str, country_code: str) -> str:
        """
        Format given phone number in E164

        :param phone: unformatted phone number
        :param country_code: country code of phone number
        :return: formatted phone number
        """
        locale = phonenumbers.parse(phone, country_code.upper())
        return phonenumbers.format_number(locale, phonenumbers.PhoneNumberFormat.E164)
