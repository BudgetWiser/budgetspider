# -*- coding: utf-8 -*-
import os
import re
from mongoengine import connect
from mongoengine.context_managers import switch_collection
from documents import Budgetspider, Cleanplus, Opengov


__rootdir = os.path.dirname(os.path.realpath(__file__)) + '/'
__mongoserver = "localhost"
__mongoport = 27017
__dbname = "budgetspider"
__colbudgetspider = "budgetspider"
__colcleanplus = "cleanplus"
__colopengov = "opengov"

connect(__dbname, host=__mongoserver, port=__mongoport)

# Lambda function for unicode UTF-8 encoding
def utf8 (x) : return x.encode('utf-8')


def calc_sum():
    years = ['2014']
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for y in years:
            total_assigned = 0
            total_summary = 0
            categories = []
            budgets_assigned = []
            budgets_summary = []
            services = []

            for i in CBudgetspider.objects(year=y):
                name = (i['category_one'], i['category_two'], i['category_three'])
                if name in categories:
                    budgets_assigned[categories.index(name)] += i['budget_assigned']
                    budgets_summary[categories.index(name)] += i['budget_summary']
                    total_assigned += i['budget_assigned']
                    total_summary += i['budget_summary']
                else:
                    categories.append(name)
                    budgets_assigned.append(i['budget_assigned'])
                    budgets_summary.append(i['budget_summary'])
                    total_assigned += i['budget_assigned']
                    total_summary += i['budget_summary']

            with open("output/category_three_"+y+".tsv", 'w') as f:
                f.write('\t'.join((utf8('category_one'), utf8('category_two'), utf8('category_three'),\
                        utf8('assigned'), utf8('summary'), utf8('num_services'))) + '\n')
                f.close()
            with open("output/services_"+y+".tsv", 'w') as f:
                f.write('\t'.join((utf8('service'), utf8('category_one'), utf8('category_two'),\
                        utf8('category_three'), utf8('assigned'), utf8('summary'))) + 'n')
                f.close()
            for i in range(len(categories)):
                if not categories[i] == None:
                    with open("output/category_three_"+y+".tsv", 'a') as f:
                        num_services = CBudgetspider.objects(year=y, category_one=utf8(categories[i][0]),\
                                category_two=utf8(categories[i][1]), category_three=utf8(categories[i][2])).count()
                        f.write("\t".join((utf8(categories[i][0]), utf8(categories[i][1]), utf8(categories[i][2]),\
                                str(budgets_assigned[i]), str(budgets_summary[i]), str(num_services))) + "\n")
                    with open("output/services_"+y+".tsv", 'a') as f:

                        for b in CBudgetspider.objects(year=y, category_one=utf8(categories[i][0]),\
                                category_two=utf8(categories[i][1]), category_three=utf8(categories[i][2])):
                            f.write("\t".join((utf8(b['service']), utf8(categories[i][0]), utf8(categories[i][1]),\
                                    utf8(categories[i][2]), str(b['budget_assigned']), str(b['budget_summary']))) + "\n")

            print "TOTAL", y, total_assigned, total_summary, len(CBudgetspider.objects(year=y)), "services"


def get_service():
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects(year='2013', category_one='일반공공행정', category_two='재정금융'):
            print utf8(i['category_three']), utf8(i['service']), i['budget_assigned']

def find_service(_service_name):
    print 'budgetspider'
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects.all():
            if utf8(re.sub('[~*()\'\". -]', '', i['service'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['service'], i['year'], i['department'], i['category_one'], i['category_two'])):
                    print pr,
    print
    print 'opengov'
    with switch_collection(Opengov, __colopengov) as COpengov:
        for i in COpengov.objects():
            if utf8(re.sub('[~*()\'\". -]', '', i['name'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['name'], i['level_one'], u['level_two'])):
                    print pr,
    print
    print 'cleanplus'
    with switch_collection(Cleanplus, __colcleanplus) as CCleanplus:
        for i in CCleanplus.objects(service=_service_name):
            if utf8(re.sub('[~*()\'\". -]', '', i['service'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['service'], i['year'], i['department'])):
                    print pr,


calc_sum()
#get_service()
#find_service('서울 시니어예술제')
