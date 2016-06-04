# -*- coding: utf-8 -*-

import urllib
import datetime
import MySQLdb

from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request

from ..items import ClienItem

from konlpy.tag import Mecab

print "============================="
print "start clien crawling..."
print "============================="

maxUrlCount = 1
currentUrlCount = 1
BOARD_NO = "1"

#크롤링할 URL
domain = u"http://clien.net"
urlPath = u"/cs2/bbs/board.php?bo_table=park"
page = u"&page="
url = domain + urlPath + page + u"1"

mecab = Mecab()

class ClienSpider(Spider):
    name = "clien"
    allowed_domains = ["clien.net"]
    start_urls = [url]

    print "start :" + str(start_urls)

    def parse(self, response):
        global currentUrlCount, maxUrlCount

        print "parsing pagelink..." + str(currentUrlCount)

        hxs = Selector(response)

        posts =[]
        posts = hxs.xpath('//tr[@class="mytr"]')
        print "posts count: " + str(len(posts))

        LastPostTime = self.getLastPostTime()

        items = []
        for post in posts:
            item = ClienItem()
            item['title'] = post.xpath('td[@class="post_subject"]/a[1]/text()').extract()[0]
            item['dateTime'] = datetime.datetime.strptime(''.join(post.xpath('td[4]/span[1]/@title').extract()), '%Y-%m-%d %H:%M:%S')
            # item['hit'] = ''.join(post.xpath('td[5]/text()').extract())
            item['sourceUrl'] = domain + u'/cs2' + post.xpath('td[@class="post_subject"]/a[1]/@href').extract()[0].strip('..')
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

        contents = ''.join(hxs.select('//span[@id="writeContents"]').extract())
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
