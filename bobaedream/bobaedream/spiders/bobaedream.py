# -*- coding: utf-8 -*-

import urllib
import datetime
import MySQLdb

from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request

from ..items import BobaedreamItem

from konlpy.tag import Mecab

print "============================="
print "start Bobaedream crawling..."
print "============================="

maxUrlCount = 1
currentUrlCount = 1
BOARD_NO = "5"

#크롤링할 URL
domain = u"http://www.bobaedream.co.kr/"
urlPath = u"/list?code=freeb"
page = u"&page="
url = domain + urlPath + page + u"1"

mecab = Mecab()

class BobaedreamSpider(Spider):
    name = "bobaedream"
    allowed_domains = ["www.bobaedream.co.kr"]
    start_urls = [url]

    print "start :" + str(start_urls)

    def parse(self, response):
        global currentUrlCount, maxUrlCount

        print "parsing pagelink..." + str(currentUrlCount)

        hxs = Selector(response)

        posts =[]
        posts = hxs.xpath('//tr[@itemtype="http://schema.org/Article"]')
        print "posts count: " + str(len(posts))

        LastPostTime = self.getLastPostTime()

        items = []
        for post in posts:
            item = BobaedreamItem()
            item['title'] = post.xpath('td[2]/a[1]/text()').extract()[0]
            item['dateTime'] = datetime.datetime.strptime(str(datetime.datetime.now())[0:11] + ''.join(post.xpath('td[4]/text()').extract()) + ":00", '%Y-%m-%d %H:%M:%S')
            item['sourceUrl'] = domain + post.xpath('td[2]/a[1]/@href').extract()[0]
            item['contents'] = self.contentsParse(item['sourceUrl'])
            item['keywords'] = self.keywordsParse(item['title'], item['contents'])

            if LastPostTime >= item['dateTime'] :
                break;

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

        contents = ''.join(hxs.select('//div[@itemprop="articleBody"]').extract())
        contents = contents.replace("'",'"')

        return contents

    def keywordsParse(self, title, contents) :
        titleKeywords = mecab.nouns(title)
        contentsKeywords = mecab.nouns(contents)

        keywords = titleKeywords + contentsKeywords

        return list(set(keywords))

    def getLastPostTime(self):
        db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="hub", charset="utf8")
        cursor = db.cursor()

        try :
            query = ("SELECT MAX(post_time) FROM article where board_no =" + BOARD_NO + ";")
            cursor.execute(query)
            return cursor.fetchone()[0]
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
