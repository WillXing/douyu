# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
import datetime

class DouyustatPipeline(object):
    def __init__(self):
        file_name = datetime.datetime.now().strftime("%Y-%m-%d");
        self.file = codecs.open(file_name + ".json", "w", "utf-8")
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line.decode("unicode_escape"))
        return item
    def spider_closed(self, spider):
        self.file.close()