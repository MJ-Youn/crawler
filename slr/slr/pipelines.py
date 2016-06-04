# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb

BOARD_NO = "4"

class SlrPipeline(object):
    def __init__(self) :
        # MySQLdb.connect('디비주소', '계정 아이디', '계정 비밀번호', '디비이름')
        self.db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="hub", charset="utf8")
        self.cursor = self.db.cursor()

    def process_item(self, item, spider):
        try :
            query = ("INSERT INTO article(title, url, content, post_time, board_no)"
                + "VALUES('"\
                + item['title'].encode('utf-8') + "','"\
                + item['sourceUrl'].encode('utf-8') + "','"\
                + item['contents'].encode('utf-8') + "','"\
                + str(item['dateTime']).encode('utf-8') + "',"
                + BOARD_NO + ");"
            )

            self.cursor.execute(query)
            article_no = self.cursor.lastrowid

            for keyword in item['keywords'] :
                query = (
                    "INSERT INTO keyword(word) VALUES('" + keyword + "')"
                    + "ON DUPLICATE KEY UPDATE count = count + 1;"
                )

                self.cursor.execute(query)
                keyword_no = self.cursor.lastrowid

                query = (
                    "INSERT INTO article_keyword(article_no, keyword_no) VALUES("
                    + str(article_no) + ","
                    + str(keyword_no) + ");"
                )

                self.cursor.execute(query)

            self.db.commit()

            print "Success"
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
