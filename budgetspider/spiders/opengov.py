# -*- coding: utf-8 -*-
from scrapy import Spider, Request
from scrapy.selector import HtmlXPathSelector
from budgetspider.items import OpengovItem

class BudgetCategorySpider(Spider):
    name = "opengov"
    allowed_domains = [
        "opengov.seoul.go.kr",
    ]
    start_urls = ["http://opengov.seoul.go.kr/section/list"]

    '''
    def parse(self, response):
        # Look for other broad categories
        for category in response.xpath('//a[@class="sf-depth-2"]'):
            link = category.xpath('@href').extract()[0]
            if link.startswith("/section/list/nb"):
                url = "http://opengov.seoul.go.kr" + category.xpath('@href').extract()[0]
                yield Request(url, callback=self.parse_items)
    '''


    def parse(self, response):
        #for td in response.xpath('//td[@class="aLeft"]'):
        for tr in response.xpath('//tbody/tr'):
            td = tr.xpath('td[@class="aLeft"]')
            url = "http://opengov.seoul.go.kr" + td.xpath('a/@href').extract()[0]
            item = OpengovItem()
            item['index'] = tr.xpath('td[@class="hide-mobile"]/text()').extract()[0].strip()
            print item['index']
            request = Request(url, callback=self.parse_category)
            request.meta['item'] = item
            yield request

        curr_page = response.xpath('//li[@class="pager-current"]')
        if len(curr_page) == 0:
            curr_page = response.xpath('//li[@class="pager-current first"]')
        if len(curr_page) == 0:
            return
        curr_page = curr_page.xpath('text()').extract()[0]

        for next_page in response.xpath('//li[@class="pager-item"]'):
            if int(next_page.xpath('a/text()').extract()[0]) > int(curr_page):
                url = "http://opengov.seoul.go.kr" + next_page.xpath('a/@href').extract()[0]
                yield Request(url, callback=self.parse)


    def parse_category(self, response):
        categories = response.xpath('//td[@itemprop="contentLocation"]/text()').extract()[0].strip('\n').strip('\t').split(' > ')
        item = response.meta['item']
        item['service'] = response.xpath('//h2[@itemprop="name"]/text()').extract()[0]
        item['entry_date'] = response.xpath('//td[@itemprop="contributor"]/text()').extract()[0].strip()
        item['category_one'] = categories[0]
        item['category_two'] = categories[1]
        item['category_three'] = categories[2]
        yield item
