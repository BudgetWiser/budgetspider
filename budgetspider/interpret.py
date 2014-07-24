# -*- coding: utf-8 -*-
import json
import os
import datetime
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


# Labmda function for unicde UTF-8 encoding
def utf8code (x) : return x.encode('utf-8')

def main():
    connect(__dbname, host=__mongoserver, port=__mongoport)
    cleanplus, opengov = [], []

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

    # Reduce duplcate entries in cleanplus
    print "Reducing duplicate entries"
    for d in cleanplus:
        del d['index']
    rm_list = []
    for d in range(len(cleanplus)-1):
        if cleanplus[d] in cleanplus[d+1:]:
            print 'DUP', cleanplus[d]['service'], cleanplus[d]['year'], cleanplus[d]['department'], cleanplus[d]['team']
            with open('error/duplicate.txt', 'a') as f:
                f.write("\t".join(map(utf8code, (cleanplus[d]['service'], cleanplus[d]['year'], cleanplus[d]['department'], cleanplus[d]['team'], cleanplus[d]['budget_assigned'], "\n"))))
            rm_list.append(d)
        if d%1000 == 0:
            print "Reduced", d, "entries"
    rm_list.reverse()
    print "Handling", len(rm_list), "duplicate entries"
    for i in rm_list:
        del cleanplus[i]
    print "Reduced to", len(cleanplus)

    # Read from opengov scraped json file
    with open(__rootdir + __dirname + __opengov + __extension) as f:
        opengov = json.load(f)
        print "Reading from", __dirname + __opengov + __extension, len(opengov)

    # Remove duplciate entries in opengov
    rm_list = []
    for d in range(len(opengov)-1):
        if opengov[d] in opengov[d+1:]:
            print 'DUP', opengov[d]['name'], opengov[d]['level_one'], opengov[d]['level_two'], opengov[d]['level_three']
            rm_list.append(d)
        if d%1000 == 0:
            print d
    rm_list.reverse()
    print "Handling", len(rm_list), "duplicate entries"
    for i in rm_list:
        del opengov[i]
    print "Reduced to", len(opengov)

    # Log cleanplus DB entry
    with switch_collection(Cleanplus, __colcleanplus) as CCleanplus:
        num_saved, num_unsaved = 0, 0

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
                print data, len(CCleanplus.objects.all())
                print c['service'], c['year'], c['department'], c['team'], c['budget_summary']


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

    # Log budgetspider DB entry
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        num_404, num_searched = 0, 0
        for c in cleanplus:
            found = False
            num_searched += 1
            for o in opengov:
                if "".join(o['name'].split()) == "".join(c['service'].split()):
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
                        data.save()
                    except:
                        print data, len(CBudgetspider.objects.all())
                        print c['service'], o['level_one'], o['level_two'], o['level_three']
                    break
            if not found:
                with open("error/unmatched.tsv", 'a') as f:
                    err = "\t".join((c['service'], c['year'], c['department'], c['team'], c['budget_summary'])).encode('utf-8')
                    print err
                    f.write(err + '\n')
                    num_404 += 1
        print num_404, "items are not matched"


main()
