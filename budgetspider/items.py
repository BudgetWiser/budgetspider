# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CleanplusItem(scrapy.Item):
    index = scrapy.Field()
    year = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    department = scrapy.Field()
    team = scrapy.Field()
    service = scrapy.Field()
    budget_summary = scrapy.Field() # Budget for service activity
    budget_assigned = scrapy.Field() # Budget assigned for this year
    budget_current = scrapy.Field() # Budget assigned for this year + Budget carried + Budget increment/decrement
    budget_contract = scrapy.Field()
    budget_spent = scrapy.Field()

class OpengovItem(scrapy.Item):
    index = scrapy.Field()
    service = scrapy.Field()
    entry_date = scrapy.Field()
    category_one = scrapy.Field()
    category_two = scrapy.Field()
    category_three = scrapy.Field()
