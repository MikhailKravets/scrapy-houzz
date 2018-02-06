import multiprocessing

from scrapy.crawler import CrawlerProcess
import houzz.settings as settings
from houzz.spiders import APISpider


def crawl(params, queue):
    process = CrawlerProcess(params)
    process.crawl(APISpider, queue=queue)
    process.start()


params = {}
for k in dir(settings):
    params[k] = getattr(settings, k)

total_max = params['MAX_COUNT']
step = total_max // 5
start_from = params['START_FROM']

q = multiprocessing.Manager().Queue()
with multiprocessing.Pool(5) as pool:
    args = []
    for i in range(start_from, total_max, step):
        d = params.copy()
        d['START_FROM'] = i
        d['MAX_COUNT'] = i + step
        args.append((d, q))
    pool.starmap(crawl, args)