# -*- coding: utf-8 -*-

import urllib

from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request

from ..items import TodayhumorItem

from konlpy.tag import Mecab

print "============================="
print "start Todayhumor crawling..."
print "============================="

maxUrlCount = 1
currentUrlCount = 1

#크롤링할 URL
domain = u"http://www.todayhumor.co.kr"
urlPath = u"/board/list.php?table=freeboard"
page = u"&page="
url = domain + urlPath + page + u"1"

mecab = Mecab()

class TodayhumorSpider(Spider):
    name = "todayhumor"
    allowed_domains = ["www.todayhumor.co.kr"]
    start_urls = [url]

    print "start :" + str(start_urls)

    def parse(self, response):
        global currentUrlCount, maxUrlCount

        print "parsing pagelink..." + str(currentUrlCount)

        print response
        print urllib.urlopen(url).read()

        hxs = Selector(response)

        posts =[]
        posts = hxs.xpath('//tr[@class="list_tr_freeboard"]')
        print "posts count: " + str(len(posts))

        items = []
        for post in posts:
            item = TodayhumorItem()
            item['title'] = post.xpath('td[@class="subject"]/a[1]/text()').extract()[0]
            item['dateTime'] = ''.join(post.xpath('td[@class="date"]/text()').extract())
            item['sourceUrl'] = domain + post.xpath('td[@class="subject"]/a[1]/@href').extract()[0]
            item['contents'] = self.contentsParse(item['sourceUrl'])
            item['keywords'] = self.keywordsParse(item['title'], item['contents'])
            yield item

        if(currentUrlCount < maxUrlCount):
            currentUrlCount = currentUrlCount + 1
            nextPage = [domain + urlPath + page + str(currentUrlCount)]
            print '------->request url :' + nextPage[0]
            yield Request(nextPage[0], self.parse)

        # return items

    def contentsParse(self, url):
        data = urllib.urlopen(url).read()
        hxs = Selector(text=data)

        contents = ''.join(hxs.select('//div[@class="viewContent"]').extract())
        contents = contents.replace("'",'"')

        return contents

    def keywordsParse(self, title, contents) :
        titleKeywords = mecab.nouns(title)
        contentsKeywords = mecab.nouns(contents)

        keywords = titleKeywords + contentsKeywords

        return list(set(keywords))
