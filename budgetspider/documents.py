import mongoengine

class Cleanplus(mongoengine.Document):
    service = mongoengine.StringField()
    year = mongoengine.StringField()
    start_date = mongoengine.DateTimeField()
    end_date = mongoengine.DateTimeField()
    department = mongoengine.StringField()
    team = mongoengine.StringField(unique_with=('service', 'year', 'department', 'team'))
    budget_summary = mongoengine.LongField()
    budget_assigned = mongoengine.LongField()
    budget_current = mongoengine.LongField()
    budget_contract = mongoengine.LongField()
    budget_spent = mongoengine.LongField()

    meta = {
        'indexes': [
            'service',
            'year',
            ('department', 'team'),
            'budget_summary'
        ]
    }


class Opengov(mongoengine.Document):
    service = mongoengine.StringField(unique_with=('service', 'category_one', 'category_two', 'category_three'))
    category_one = mongoengine.StringField()
    category_two = mongoengine.StringField()
    category_three = mongoengine.StringField()

    meta = {
        'indexes': [
            'service',
            ('category_one', 'category_two', 'category_three')
        ]
    }


class Budgetspider(mongoengine.Document):
    service = mongoengine.StringField()
    year = mongoengine.StringField()
    start_date = mongoengine.DateTimeField()
    end_date = mongoengine.DateTimeField()
    department = mongoengine.StringField()
    team = mongoengine.StringField(unique_with=('service', 'year', 'category_three', 'department', 'team'))
    category_one = mongoengine.StringField()
    category_two = mongoengine.StringField()
    category_three = mongoengine.StringField()
    budget_summary = mongoengine.LongField()
    budget_assigned = mongoengine.LongField()
    budget_current = mongoengine.LongField()
    budget_contract = mongoengine.LongField()
    budget_spent = mongoengine.LongField()

    meta = {
        'indexes': [
            'service',
            'year',
            ('department', 'team'),
            ('category_one', 'category_two', 'category_three'),
            'budget_summary'
        ]
    }
