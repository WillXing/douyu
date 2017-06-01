# -- coding: UTF-8
import datetime
import logging
import re
import time

import MySQLdb as mdb
import scrapy
from scrapy.shell import inspect_response


class DouYuSpider(scrapy.Spider):
  name="douyu"
  basic_url="https://www.douyu.com/directory/all?isAjax=2&page="
  current=1
  totalPage=90
  result = {}

  DB_NAME = "spider"
  TABLE_NAME = "live"

  def start_requests(self):
    self.logger.log(logging.DEBUG, "****************** START ******************")
    yield scrapy.Request(self.basic_url + str(self.current), callback=self.parse);

  def parse(self, response):

    live_list = response.css("li")
    

    for live in live_list:
      category = live.css(".tag::text").extract()[0]
      audience_str = live.css(".dy-num::text").extract()[0]
      audience_num = 0
      # inspect_response(response, self)
      unitMatch = re.search(ur'(\d*\.?\d*)\u4e07', audience_str)
      
      if unitMatch is not None:
        audience_num = int( float( unitMatch.group(1) ) * 10000 )
      else:
        audience_num = int(audience_str)

      if category in self.result:
        self.result[category]["audience_total"] += audience_num
        self.result[category]["live_num_total"] += 1
      else:
        self.result[category] = {"audience_total": audience_num, "live_num_total": 1}

    if self.current >= self.totalPage:
      conn = mdb.connect("localhost", "root", "")
      conn.select_db(self.DB_NAME)
      cursor = conn.cursor()
      
      date = datetime.datetime.now()
      
      insert_set = []
      for key in self.result.keys():
        audience_total = self.result[key]["audience_total"]
        live_num_total = self.result[key]["live_num_total"]
        
        date_to_insert = date.strftime("%Y-%m-%d")
        hour_to_insert = date.strftime("%H")
        record_existing = cursor.execute("SELECT * FROM "+ self.TABLE_NAME +" WHERE name=%s AND date=%s AND hour=%s LIMIT 1", (key, date_to_insert, hour_to_insert)) > 0
        if record_existing:
          cursor.execute("UPDATE "+ self.TABLE_NAME +" SET audience_total=%s, live_num_total=%s WHERE name=%s AND date=%s AND hour=%s", (audience_total, live_num_total, key, date_to_insert, hour_to_insert))
        else:
          cursor.execute("INSERT INTO "+ self.TABLE_NAME +" values(%s,%s,%s,%s,%s)", (date_to_insert, hour_to_insert, key, audience_total, live_num_total))
        # date/ hour/ name/ audience_total/ live_num_total
      # load data infile "/Users/xnhuang/Documents/projects/spider/douyustat/douyustat/spiders/2017-06-01csv" into table douyu_tmp fields terminated by ";" enclosed by '"' lines terminated by '\n' ignore 1 rows
      conn.commit()
      conn.close()
      return

    self.current = self.current + 1;
    time.sleep(3) #incase robotic check
    yield scrapy.Request(self.basic_url + str(self.current), callback=self.parse)
