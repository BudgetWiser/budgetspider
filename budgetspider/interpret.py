# -*- coding: utf-8 -*-
import json
import os
import re
import datetime
from bisect import bisect_left
from mongoengine import connect
from mongoengine.context_managers import switch_collection
from documents import Cleanplus, Opengov, Budgetspider

# JSON files settings
__rootdir = os.path.dirname(os.path.realpath(__file__)) + '/'
__dirname = "json/"
#__cleanplus = "2013" # cp_XXXXXXXX
__opengov = "opengov"   # og_XXXXXXXX
__extension = ".json"

__cleanlist = ["20140719", "2013", "2012", "2011", "2010", "2009", "2008"]

# PyMongo DB settings
__mongoserver = "localhost"
__mongoport = 27017
__dbname = "budgetspider"
__colbudgetspider = "budgetspider"
__colopengov = "opengov"
__colcleanplus = "cleanplus"
connect(__dbname, host=__mongoserver, port=__mongoport)

# Data JSON objects
cleanplus, opengov = [], []


# Labmda function for unicde UTF-8 encoding
def utf8 (x) : return x.encode('utf-8')
def rm_spchar (x) : return re.sub('[~*()\'\"., -]', '', x)


def read_cleanplus():
    global cleanplus
    # Read from cleanplus scraped json file
    for __cleanplus in __cleanlist:
        with open(__rootdir + __dirname + __cleanplus + __extension) as f:
            cleanplus.extend(json.load(f))
            print "Reading from", __dirname + __cleanplus + __extension, len(cleanplus)

    # Populated unpopulated cleanplus json objects
    for c in cleanplus:
        if c['start_date'] == '':
            c['start_date'] = datetime.date(datetime.MINYEAR, 1, 1)
        if c['end_date'] == '':
            c['end_date'] = datetime.date(datetime.MINYEAR, 1, 1)
        if c['budget_assigned'] == '':
            c['budget_assigned'] = str(0)
        if c['budget_current'] == '':
            c['budget_current'] = str(0)
        if c['budget_contract'] == '':
            c['budget_contract'] = str(0)
        if c['budget_spent'] == '':
            c['budget_spent'] = str(0)

    # Quicksort
    def quicksort(arr):
       less = []
       pivotList = []
       more = []
       if len(arr) <= 1:
           return arr
       else:
           pivot = arr[0]['service']
           for i in arr:
               if i['service'] < pivot:
                   less.append(i)
               elif i['service'] > pivot:
                   more.append(i)
               else:
                   pivotList.append(i)
           less = quicksort(less)
           more = quicksort(more)
           return less + pivotList + more
    
    # Clear previous duplicate log
    with open('error/duplicate_cleanplus.tsv', 'w') as f:
        pass

    # Reduce duplcate entries in cleanplus
    print "Reducing duplicate entries"
    for d in cleanplus:
        del d['index']
    cleanplus = quicksort(cleanplus)
    rm_list = []
    for d in range(len(cleanplus)-1):
        if cleanplus[d] == cleanplus[d+1]:
            with open('error/duplicate_cleanplus.tsv', 'a') as f:
                f.write("\t".join(map(utf8, (cleanplus[d]['service'], cleanplus[d]['year'], cleanplus[d]['department'], cleanplus[d]['team'], cleanplus[d]['budget_assigned'], "\n"))))
            rm_list.append(d)
    rm_list.reverse()
    print "Handling", len(rm_list), "duplicate entries"
    for i in rm_list:
        del cleanplus[i]
    print "Reduced to", len(cleanplus)


def read_opengov():
    global opengov
    # Read from opengov scraped json file
    with open(__rootdir + __dirname + __opengov + __extension) as f:
        opengov = json.load(f)
        print "Reading from", __dirname + __opengov + __extension, len(opengov)

    # Quicksort
    def quicksort(arr):
       less = []
       pivotList = []
       more = []
       if len(arr) <= 1:
           return arr
       else:
           pivot = arr[0]['name']
           for i in arr:
               if i['name'] < pivot:
                   less.append(i)
               elif i['name'] > pivot:
                   more.append(i)
               else:
                   pivotList.append(i)
           less = quicksort(less)
           more = quicksort(more)
           return less + pivotList + more

    # Clear previous duplicate log
    with open('error/duplicate_opengov.tsv', 'w') as f:
        pass

    # Remove duplciate entries in opengov
    opengov = quicksort(opengov)
    rm_list = []
    for d in range(len(opengov)-1):
        if opengov[d] == opengov[d+1]:
            with open('error/duplicate_opengov.tsv', 'a') as f:
                f.write("\t".join(map(utf8, (opengov[d]['name'], opengov[d]['level_one'], opengov[d]['level_two'], opengov[d]['level_three'], "\n"))))
            rm_list.append(d)
    rm_list.reverse()
    print "Handling", len(rm_list), "duplicate entries"
    for i in rm_list:
        del opengov[i]
    print "Reduced to", len(opengov)
    

def log_cleanplus():
    # Log cleanplus DB entry
    with switch_collection(Cleanplus, __colcleanplus) as CCleanplus:
        num_saved, num_unsaved = 0, 0
        unsaved = []

        for c in cleanplus:
            data = CCleanplus(
                service = c['service'],
                year = c['year'],
                department = c['department'],
                team = c['team'],
                start_date = c['start_date'],
                end_date = c['end_date'],
                budget_summary = c['budget_summary'],
                budget_assigned = c['budget_assigned'],
                budget_current = c['budget_assigned'],
                budget_contract = c['budget_contract'],
                budget_spent = c['budget_spent']
            )

            try:
                data.save()
                num_saved += 1
            except:
                print c['service'], c['year'], c['department'], c['team'], c['budget_summary'], len(CCleanplus.objects.all())
                unsaved.append(d)
                num_unsaved += 1


def log_opengov():
    # Log opengov DB entry
    with switch_collection(Opengov, __colopengov) as COpengov:
        for o in opengov:
            data = COpengov(
                service = o['name'],
                category_one = o['level_one'],
                category_two = o['level_two'],
                category_three = o['level_three']
            )

            try:
                data.save()
            except:
                print data, len(COpengov.objects.all())
                print c['service'], o['level_one'], o['level_two'], o['level_three']


# Simulate logging budgetspider DB entries
def simlog_budgetspider():
    # Clear previous unmatched records
    with open("error/unmatched.tsv", 'w') as f:
        pass

    # Small opengov objects
    sopengov = []
    for i in opengov:
        sopengov.append(i['name'])

    # Binary search
    def bsearch(a, x, lo=0, hi=None):
        hi = hi or len(a)
        pos = bisect_left(a, x, lo, hi)
        return (pos if pos != hi and a[pos] == x else -1)

    num_matched, num_unmatched = 0, 0
    unmatched = []
    for c in cleanplus:
        if int(c['year']) < 2010:
            continue
        search_idx = bsearch(sopengov, c['service'])
        if search_idx != -1 and c['service'] == opengov[search_idx]['name']:
            num_matched += 1
        else:
            num_unmatched += 1
            unmatched.append(c)

    for i in sopengov:
        i = utf8(rm_spchar(i.strip())).split('ㆍ')

    for i in unmatched:
        search_idx = bsearch(sopengov, utf8(rm_spchar(i['service'].strip())).split('ㆍ'))
        if search_idx != -1:
            print sopengov[search_idx], i['service']
            num_matched += 1
            num_unmatched -= 1
        else:
            print 'NOMATCH', i['year'], utf8(rm_spchar(i['service'].strip())).split('ㆍ')[0]

    # Check text similarity
    # TODO

    print num_matched, num_unmatched
        
        


def log_budgetspider():
    # Clear previous unmatched record
    with open("error/unmatched.tsv", 'w') as f:
        pass

    # Log budgetspider DB entry
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        num_matched, num_unmatched, num_unsaved = 0, 0, 0
        matched, unmatched, unsaved = [], [], []
        for c in cleanplus:
            if int(c['year']) < 2010:
                continue
            found = False
            for o in opengov:
                if "".join(utf8(rm_spchar(o['name'].strip())).split('ㆍ')) == "".join(utf8(rm_spchar(c['service'].strip())).split('ㆍ')):
                    found = True
                    data = CBudgetspider(
                        service = c['service'],
                        year = c['year'],
                        start_date = c['start_date'],
                        end_date = c['end_date'],
                        department = c['department'],
                        team = c['team'],
                        category_one = o['level_one'],
                        category_two = o['level_two'],
                        category_three = o['level_three'],
                        budget_summary = c['budget_summary'],
                        budget_assigned = c['budget_assigned'],
                        budget_current = c['budget_current'],
                        budget_contract = c['budget_contract'],
                        budget_spent = c['budget_spent']
                    )
                    try:
                        # data.save()
                        num_matched += 1
                        matched.append((c, o))
                    except:
                        # print c['service'], o['level_one'], o['level_two'], o['level_three']
                        num_unsaved += 1
                        unsaved.append((c, o))
                    break
            if not found:
                with open("error/unmatched.tsv", 'a') as f:
                    print 'NOMATCH', "\t".join(utf8(rm_spchar(c['service'])).split('ㆍ')), len(CBudgetspider.objects.all())
                    err = "\t".join((c['service'], c['year'], c['department'], c['team'], c['budget_summary'])).encode('utf-8')
                    print err
                    f.write(err + '\n')
                    unmatched.append(c)
                    num_unmatched += 1
        print num_unmatched, "items are not matched"


read_cleanplus()
read_opengov()
#log_cleanplus()
#log_opengov()
simlog_budgetspider()
#log_budgetspider()
