GA_TO_BQ = {'user': 'fullVisitorId',
            'session': '''CONCAT(fullVisitorId, '-', cast(visitId AS string), '-', date)''',
            'date': 'date',
            'page': 'hits.page.pagePath',
            'event category': 'hits.eventInfo.eventCategory',
            'event action': 'hits.eventInfo.eventAction',
            'event label': 'hits.eventInfo.eventLabel',
            'campaign': 'trafficSource.campaign',
            'source': 'trafficSource.source',
            'medium': 'trafficSource.medium',
            'keyword': 'trafficSource.keyword',
            'device category': 'device.deviceCategory',
            'screen resolution': 'device.screenResolution',
            'browser': 'device.browser',
            'operating system': 'device.operatingSystem',
            'transactions': 'hits.transaction.transactionId',
            'affiliation': 'hits.transaction.affiliation',
            'revenue': ' hits.transaction.transactionRevenue / 1000000',
            'product': 'product.V2ProductName',
            'product revenue': 'product.productRevenue / 1000000',
            'product detail view': 'product.V2ProductName',
            }

UNNEST_LOOKUP = {
            r'^hits\.': ['UNNEST(hits) AS hits'],
            r'^product\.': ['UNNEST(hits) AS hits', 'UNNEST(hits.product) AS product'],
            r'^promotion\.': ['UNNEST(hits) AS hits', 'UNNEST(hits.promotion) AS promotion']
        }