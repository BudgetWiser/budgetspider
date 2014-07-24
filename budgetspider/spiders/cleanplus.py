# -*- coding: utf-8 -*-
from scrapy import Spider, Request
from scrapy.http import FormRequest
from budgetspider.items import CleanplusItem
from datetime import date

class BudgetspiderSpider(Spider):
    name = 'cleanplus'
    allowed_domains = [
        "cleanplus.seoul.go.kr",
    ]
    start_urls = [
        "http://cleanplus.seoul.go.kr/saupinfo/SaupInfoNewList.do?method=list"
    ]


    def parse(self, response):
        yield FormRequest.from_response(
            response,
            formname="SaupInfoNewActionForm",
            formdata={
                'Search_FIS_YEAR': '2014',
            },
            callback=self.parse_content
        ).replace(url="http://cleanplus.seoul.go.kr/saupinfo/SaupInfoNewList.do")


    def parse_content(self, response):
        form = response.xpath('//form[@name="SaupInfoNewActionForm"]')

        # parse city service items
        for tr in form.xpath('//table[@class="tablelist"]/tbody/tr'):
            scrap_num = tr.xpath('td[@class="no"]/text()').extract()[0].strip()
            scrap_yr = tr.xpath('td[@class="year"]/text()').extract()
            scrap_start = scrap_yr[1].strip().split('~')[0].split('.')
            scrap_end = scrap_yr[1].strip().split('~')[1].split('.')
            scrap_title = tr.xpath('td[@class="tit"]/text()').extract()
            scrap_dept = scrap_title[0].strip().split(' ')
            scrap_bdgt_summary = _parse_number(tr.xpath('td[@class="money"]/text()').extract()[0].strip())
            scrap_service = scrap_title[1].strip()

            item = CleanplusItem()
            item['index'] = scrap_num
            item['year'] = scrap_yr[0].strip()
            try:
                item['start_date'] = date(int(scrap_start[0]), int(scrap_start[1]), int(scrap_start[2]))
            except:
                print 'START', scrap_start
                item['start_date'] = ''
            try:
                item['end_date'] = date(int(scrap_end[0]), int(scrap_end[1]), int(scrap_end[2]))
            except:
                print 'END', scrap_end
                item['end_date'] = ''
            item['department'] = scrap_dept[0]
            try:
                item['team'] = scrap_dept[1]
            except:
                print 'DEPT', scrap_dept
                item['team'] = ''
            item['budget_summary'] = "".join(scrap_bdgt_summary.split(','))

            scrap_onclick = tr.xpath('attribute::onclick').extract()[0].split("'")
            fiscal_year = scrap_onclick[1]
            detail_code = scrap_onclick[3]

            request = FormRequest.from_response(
                response,
                formname="SaupInfoNewActionForm",
                formdata={
                    'method': 'detail',
                    'FIS_YEAR': fiscal_year,
                    'DBIZ_CD': detail_code
                },
                callback=self.parse_service
            ).replace(url="http://cleanplus.seoul.go.kr/saupinfo/SaupInfoNewList.do")
            request.meta['item'] = item
            yield request

        # parse next 10 pages
        try:
            next_ten = form.xpath('//span[@class="nxt"]/img/attribute::page').extract()[1].strip()
            for i in range(1, int(next_ten)+1):
                yield FormRequest.from_response(
                    response,
                    formname="SaupInfoNewActionForm",
                    formdata={
                        'currentPage': str(i)
                    },
                    callback=self.parse
                ).replace(url="http://cleanplus.seoul.go.kr/saupinfo/SaupInfoNewList.do")
        except:
            print 'ERROR retrieving next pages from ', form.xpath('//span[@class="num"]/strong/a/font/text()').extract()

            '''
        # parse other pages
        for a in form.xpath('//span[@class="num"]/a/text()').extract():
            yield FormRequest.from_response(
                response,
                formname="SaupInfoNewActionForm",
                formdata={
                    'currentPage': a.strip()
                },
                callback=self.parse
            ).replace(url="http://cleanplus.seoul.go.kr/saupinfo/SaupInfoNewList.do")
            '''


    def parse_service(self, response):
        scrap_service = response.xpath('//td[@class="no"]/text()').extract()[0]

        item = response.meta['item']
        item['service'] = scrap_service
        request = FormRequest.from_response(
            response,
            formname="SaupInfoNewActionForm",
            formdata={
                'method': 'detail01'
            },
            callback=self.parse_budget
        ).replace(url=response.url)
        request.meta['item'] = item
        yield request

    def parse_budget(self, response):
        item = response.meta['item']
        td  = response.xpath('//table[@class="tablewrite"]//td/text()').extract()
        item['budget_assigned'] = _parse_number(td[0].strip().strip(u'\xa0\uc6d0'))
        item['budget_current'] = _parse_number(td[1].strip().strip(u'\xa0\uc6d0'))
        item['budget_contract'] = _parse_number(td[2].strip().strip(u'\xa0\uc6d0'))
        item['budget_spent'] = _parse_number(td[3].strip().strip(u'\xa0\uc6d0'))
        yield item
            

def _parse_number(strin):
    return "".join(strin.split(','))
