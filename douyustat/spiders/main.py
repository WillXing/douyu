# -- coding: UTF-8
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
      for key in self.result.keys():
        each = {
          "name": key, 
          "audience_total": self.result[key]["audience_total"],
          "live_num_total": self.result[key]["live_num_total"]
        }
        yield each
      # load data infile "/Users/xnhuang/Documents/projects/spider/douyustat/douyustat/spiders/2017-06-01csv" into table douyu_tmp fields terminated by ";" enclosed by '"' lines terminated by '\n' ignore 1 rows
      return

    self.current = self.current + 1;
    time.sleep(3) #incase robotic check
    yield scrapy.Request(self.basic_url + str(self.current), callback=self.parse)
