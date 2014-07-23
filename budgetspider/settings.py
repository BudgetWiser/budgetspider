# -*- coding: utf-8 -*-

# Scrapy settings for budgetspider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'budgetspider'

SPIDER_MODULES = ['budgetspider.spiders']
NEWSPIDER_MODULE = 'budgetspider.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'budgetspider (+http://budgetwiser.org/)'

# Concurrent crawling
CONCURRENT_REQUESTS = 100
LOG_LEVEL = 'INFO'
COOKIES_ENABLED = True
AJAXCRAWL_ENABLED = True
