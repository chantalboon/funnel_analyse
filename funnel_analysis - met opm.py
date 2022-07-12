# import packages
from typing import Union

import pandas as pd
import numpy as np
from google.cloud import bigquery
# # import plotly.graph_objects as go
# # import plotly.express as px
from datetime import datetime
# import json
import pprint

# Set plotly as plotting backend
# pd.options.plotting.backend = "plotly"


# Global variables

BQ_PROJECT = "bigquery-anwb"
BQ_DATASET_MASTER = "87223461"
BQ_DATASET_GA4_test = "analytics_290476086"
BQ_DATASET_GA4_prod = "analytics_229981733"

# ---------------------------------------------------------------------------------------------------------
# Overall zie ik dat je af en toe wat omslachtig bent met lege lijsten en lastig te volgen dictionary structuren,
# kortom je dataholders- en management. Het is op zich prima hoe je het nu doet en ik ben zelf ook allesbehalve perfect,
# maar ik kan je aanbevelen om complexe datavariabelen onder te brengen in classes. Dit maakt het leesbaarder en
# duidelijker voor jezelf.
#
# Onderstaand voorbeeld bevat nog niet alle methods die je nodig zou hebben, maar geeft even een idee van hoe je dit
# zou kunnen opbouwen op een OOP manier
#
# class Funnel:
#     def __init__(self, settings: dict):
#         self.settings = settings
#         self.scope = self.set_scope()
#         self.type = settings['funnelType']
#         self.funnel_dimensions = settings['breakdownDimensions']
#         self.date_from, self.date_to = settings['dateRange']
#         ...
#         self.steps = [FunnelStep(self, step_id) for step_id in self.settings['steps']]
#
#     def set_scope(self) -> str:
#         return f"""{lookup_bq_names(self.settings['funnelScope'])}"""
#
#     def ga_to_bq(self, dimension):
#
#         ...
#         return bq_name
#
#     def get_results(self):
#         for step in self.steps:
#             step.get_result()
#
#
# class FunnelStep:
#     def __init__(self, funnel: Funnel, step_id: int):
#         self.settings = funnel
#         self.step_id = step_id
#         self.sql = self.create_sql()
#         self.result = None
#
#     def create_sql(self) -> str:
#         return f"""-- Step {self.step_id}
#                 CREATE TEMP FUNCTION customDimensionByIndexUA(indx INT64, arr ARRAY<STRUCT<index INT64,
#                               value STRING>>) AS (
#                               (   SELECT x.value FROM UNNEST(arr) x WHERE indx=x.index)
#                 );
#                 SELECT distinct {self.settings.scope} as id
#                     ,  visitStartTime + cast(hits.time/1000 as int64) as timestamp
#                     ,  EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + cast(hits.time/1000 as int64)) AT
#                               TIME ZONE "Europe/Amsterdam") as datetime
#                     ,  {self.settings.funnel_dimensions}
#         ...
#         """
#
#     def get_result(self):
#         client = bigquery.Client()
#         client.query(self.sql)
#         ...
# ---------------------------------------------------------------------------------------------------------


def lookup_bq_names(ga_name: str):

    # ---------------------------------------------------------------------------------------------------------
    # ga_to_bq is een constante die nu elke keer opnieuw gedefinieerd wordt -> losse file voor constants om het
    # overzichtelijk te houden? noem het constants.py, dan from constants import *
    # ---------------------------------------------------------------------------------------------------------
    ga_to_bq = {'user': 'fullVisitorId',
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
    # ---------------------------------------------------------------------------------------------------------
    # Ik zou eigenlijk voor nu een error raisen als de variabele niet in ga_to_bq voorkomt, aangezien anders de query
    # waarschijnlijk niet gaat werken. Dus bijvoorbeeld:

    # try:
    #     bq_name = ga_to_bq[ga_name.lower()]
    # except KeyError:
    #     raise UserWarning(f'Variabele {ga_name} kan niet worden gebruikt in de query')
    #
    # return bq_name
    #
    # Als ik verder ben met de UI is handig om de error message weer te geven als user feedback (hoewel het dus nog
    # niet mogelijk is om handmatig een variabele in te voeren)
    # ---------------------------------------------------------------------------------------------------------
    if ga_name.lower() in ga_to_bq:
        bq_name = ga_to_bq[ga_name.lower()]
    else:
        bq_name = ga_name

    return bq_name


def process_filters(filter_: dict):
    # ---------------------------------------------------------------------------------------------------------
    # Om het voor jezelf makkelijker te maken, zou ik in dit soort situaties beginnen met het toewijzen van de keys aan
    # variabelen:
    #
    # dimension, condition, value = filter_.values()
    #
    # if condition == 'regex:'
    #     value = value.replace('\\', '\\\\')
    #     ....
    #
    # Verder niks meer aan doen
    # ---------------------------------------------------------------------------------------------------------

    if filter_['condition'] == 'regex':
        filter_['value'] = filter_['value'].replace('\\', '\\\\')
        filter_sql = f"""REGEXP_CONTAINS({filter_['dimension']},'{filter_['value']}')"""
    elif filter_['condition'] == 'in':
        filter_sql = f"""{filter_['dimension']} {filter_['condition']} ({filter_['value']})"""
    else:
        filter_sql = f"""{filter_['dimension']} {filter_['condition']} '{filter_['value']}'"""
    return filter_sql


def check_unnests(dimension):
    """"
    Bepaal welke items ge-unnest moeten worden
    """

    # @Robbert: dit moet mooier kunnen... toch??
    # Omdat ik nog niet weet hoe ik kan bepalen op welk niveau een customdimension is gedefinieerd,
    # wordt deze nog niet meegenomen in unnest

    # ---------------------------------------------------------------------------------------------------------
    # Je zou de hele lijst in een keer kunnen invoeren en alleen de unieke unnests kunnen overhouden.
    # Onderstaand met regex is schaalbaarder, aangezien je alleen een regel aan de lookup hoeft toe te voegen als je
    # meer unnests nodig hebt, bijboorbeeld voor de CDs en CMs.
    # Je moet hiervoor wel de ga-bq lookup aanpassen om promotion en product te hebben ipv hits.promotion en
    # hits.product, met bijkomend voordeel dat je het 1-op-1 kan overnemen in het script
    # Overigens zou ik ook deze lookup verplaatsen naar een aparte constants.py file
    #
    # def check_unnests(dimensions: list) -> list:
    #     import re
    #
    #     UNNEST_LOOKUP = {
    #         r'^hits\.': ['UNNEST(hits) AS hits'],
    #         r'^product\.': ['UNNEST(hits) AS hits', 'UNNEST(hits.product) AS product'],
    #         r'^promotion\.': ['UNNEST(hits) AS hits', 'UNNEST(hits.promotion) AS promotion']
    #     }
    #
    #     # Als er een match is met een regex, worden de unnests van die match toegevoegd aan alle unnests
    #     all_unnests = []
    #     for dimension in dimensions:
    #         all_unnests += [unnest for regex, unnest in UNNEST_LOOKUP.items() if re.search(regex, dimension)]
    #
    #     # Of nog korter met een dubbele comprehension (maar iets minder goed leesbaar)
    #     all_unnests = [unnest for regex, unnest in unnest_lookup.items() for dimension in
    #                    dimensions if re.search(regex, dimension)]
    #
    #     # Return unieke lijst met unnests. List comprehension is om van een list of lists naar een flat list met
    #     # strings te gaan, anders krijg je unhashable type 'list' op de conversion naar set voor de ontdubbeling
    #     return list(set([item for subset_of_unnests in all_unnests for item in subset_of_unnests]))
    # ---------------------------------------------------------------------------------------------------------

    unnest_list = []
    # if dimension.startswith('customDimensions'):
    #     unnest_list.append('unnest(customDimensions) as cd')
    if dimension.startswith('hits'):
        unnest_list.append('unnest(hits) as hits')
    # if dimension.startswith('hits.customDimensions'):
    #     unnest_list.append('unnest(hits) as hits')
    #     unnest_list.append('unnest(hits.customDimensions) as hits_cd')
    if dimension.startswith('product'):
        unnest_list.append('unnest(hits) as hits')
        unnest_list.append('unnest(hits.product) as product')
    if dimension.startswith('hits.promotion'):
        unnest_list.append('unnest(hits) as hits')
        unnest_list.append('unnest(hits.promotion) as promotion')
    return unnest_list


def set_funnel_scope(funnel_definitions):
    funnel_definitions['prep']['funnelScope'] = f"""{lookup_bq_names(funnel_definitions['funnelScope'])}"""

    return funnel_definitions


def set_funnel_dimensions(funnel_definitions):
    funnel_definitions['prep']['breakdownDimensions'] = [lookup_bq_names(x)
                                                         for x in funnel_definitions['breakdownDimensions']]
    funnel_definitions['prep']['sql']['funnelDimensions'] = """
        ,  """.join(funnel_definitions['prep']['breakdownDimensions'])

    funnel_unnest = []
    for f in funnel_definitions['prep']['breakdownDimensions']:
        funnel_unnest += check_unnests(f)
    funnel_definitions['unnest'] = funnel_unnest

    return funnel_definitions


def set_funnel_date_range(funnel_definitions):
    date_from = funnel_definitions['dateRange'][0]
    date_to = funnel_definitions['dateRange'][1]
    funnel_definitions['prep']['dateFrom'] = datetime.strptime(date_from, '%m-%d-%Y').date().strftime("%Y%m%d")
    funnel_definitions['prep']['dateTo'] = datetime.strptime(date_to, '%m-%d-%Y').date().strftime("%Y%m%d")
    # print('set_funnel_date_range: ', funnel_definitions['sql'])

    return funnel_definitions


def set_funnel_filters(funnel_definitions):
    funnel_definitions['prep']['filters'] = funnel_definitions['filters']
    for f in funnel_definitions['prep']['filters']:
        f['dimension'] = lookup_bq_names(f['dimension'])

    funnel_filter_sql = [process_filters(f) for f in funnel_definitions['prep']['filters']]
    funnel_definitions['prep']['sql']['funnelFilters'] = """
    AND    """.join(funnel_filter_sql)

    return funnel_definitions


def set_funnel_steps(funnel_definitions):
    # ---------------------------------------------------------------------------------------------------------
    # Ik zou proberen de eerste twee regels en de for loop start in prep_sql te zetten, dan heb je niet een grote
    # geneste loop binnen een functie en wordt je functie wat gespecialiseerder.
    # ---------------------------------------------------------------------------------------------------------

    funnel_definitions['prep']['sql']['steps'] = []
    funnel_definitions['prep']['steps'] = funnel_definitions['steps']
    for step in funnel_definitions['prep']['steps']:
        sql_step_def = {}
        sql_step_def['stepId'] = step['stepId']
        s = []
        # voor iedere stap moeten zowel de benodigde unnest voor de hele funnel als de steps toegevoegd worden
        unnest = funnel_definitions['prep']['unnest']
        for f in step['step']:
            f['dimension'] = lookup_bq_names(f['dimension'])
            s.append(process_filters(f))
            step_sql = """
    AND    """.join(s)
            unnest += check_unnests(f['dimension'])

        sql_step_def['filters'] = step_sql
        # ---------------------------------------------------------------------------------------------------------
        # Deze manier van dedupliceren kende ik nog niet, maar ik m onthouden
        # ---------------------------------------------------------------------------------------------------------
        unnest_dedup = list(dict.fromkeys(unnest))  # deduplicate list
        sql_step_def['unnest'] = """
                , """.join(unnest_dedup)
        funnel_definitions['prep']['sql']['steps'].append(sql_step_def)

    return funnel_definitions


def prep_sql(funnel_definitions):
    funnel_definitions['prep'] = {}
    funnel_definitions['prep']['sql'] = {}
    funnel_definitions['prep']['unnest'] = []

    funnel_definitions = set_funnel_scope(funnel_definitions)
    funnel_definitions = set_funnel_dimensions(funnel_definitions)
    funnel_definitions = set_funnel_date_range(funnel_definitions)
    funnel_definitions = set_funnel_filters(funnel_definitions)
    funnel_definitions = set_funnel_steps(funnel_definitions)

    for step in funnel_definitions['prep']['sql']['steps']:
        # ---------------------------------------------------------------------------------------------------------
        # Disclaimer: volledig optioneel, maar wel leuk als je iets nieuws wil leren dat niet al te moeilijk is.
        #
        # Als je dit clean wil doen, kan je het beste een jinja template gebruiken in een losse file. Voor als je het
        # interessant vindt:
        # https://towardsdatascience.com/advanced-sql-templates-in-python-with-jinjasql-b996eadd761d
        # ---------------------------------------------------------------------------------------------------------
        step['stepsql'] = f"""-- Step {step['stepId']}
    CREATE TEMP FUNCTION customDimensionByIndexUA(indx INT64, arr ARRAY<STRUCT<index INT64, value STRING>>) AS (
    (   SELECT x.value FROM UNNEST(arr) x WHERE indx=x.index)
    );
    SELECT distinct {funnel_definitions['prep']['funnelScope']} as id
        ,  visitStartTime + cast(hits.time/1000 as int64) as timestamp
        ,  EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + cast(hits.time/1000 as int64)) AT TIME ZONE "Europe/Amsterdam") as datetime
        ,  {funnel_definitions['prep']['sql']['funnelDimensions']}
    FROM   `{BQ_PROJECT}.{BQ_DATASET_MASTER}.ga_sessions_*` s, {step['unnest']}
    WHERE  _TABLE_SUFFIX >= '{funnel_definitions['prep']['dateFrom']}'
    AND    _TABLE_SUFFIX <= '{funnel_definitions['prep']['dateTo']}'
    AND    {funnel_definitions['prep']['sql']['funnelFilters']}
    AND    {step['filters']}"""

    return funnel_definitions['prep']


# SQL statement from session data
def get_UA_data(project: str, stepId, sql: str):
    # print(sql)
    # bqclient = bigquery.Client(project=project)
    df = pd.read_csv(f'''{str(stepId)}.csv''', comment='#')
    # df = bqclient.query(sql).result().to_dataframe(create_bqstorage_client=True)
    df.to_csv(f'''{str(stepId)}.csv''', index=False)
    df.head()
    return df


def main():
    funnel_definitions = pd.read_json('funneldefinitions 2.json')
    prepped_definitions = prep_sql(funnel_definitions['funnel'])
    prev_step_id = None  # geen best practice om df zo op te bouwen, maar werkt wel
    step_dict = {}
    df_count = []
    prev_step = None
    for step in prepped_definitions['sql']['steps']:
        step_id = step['stepId']
        df_step = get_UA_data(BQ_PROJECT, step_id, step['stepsql'])
        step_dict[step_id] = df_step
        # ---------------------------------------------------------------------------------------------------------
        # Hier overschrijf je de lege lijst df_count, dus je counts worden nergens opgeslagen op dit moment. Elke
        # keer dat de functie compare_steps wordt aangeroepen, wordt de oude waarde weer overschreven.
        #
        # Ik zie zelf geen output vanuit de functie op step_dict[step_id], zoals ik uit je vraag begreep. Ik zou
        # verwachten dat als er output uit de functie is, dat dat dan step_dict[step_id]['group'] zou zijn,
        # maar dit is er niet.
        #
        # Naar mijn weten is deep=True trouwens de default value voor df.copy(), dus die kun je eventueel weglaten.
        # ---------------------------------------------------------------------------------------------------------
        df_count = compare_steps(funnel_definitions['funnel']['funnelType'],
                                 prepped_definitions['breakdownDimensions'],
                                 step_dict[step_id], prev_step)

        prev_step = step_dict[step_id].copy(deep=True)
        print(step_id)
        print(df_count)


def compare_steps(funnel_type: str, breakdown_dimensions: list, step: pd.DataFrame, prev_step: Union[pd.DataFrame,
                                                                                                     None]):
    # ---------------------------------------------------------------------------------------------------------
    # Het is best practice om df.loc[:, 'group'] = <expression> te gebruiken i.p.v. df['group'] = <expression> als je
    # een nieuwe kolom aanmaakt in een DataFrame.
    # ---------------------------------------------------------------------------------------------------------

    pivot_list = [i.split('.', 1)[-1] for i in breakdown_dimensions]
    if prev_step is None:
        df = step.copy(deep=True)
        df['group'] = 'base'
    else:
        df = pd.merge(left=prev_step.copy(deep=True), right=step.copy(deep=True), how='outer',
                      on=['id'] + pivot_list,
                      suffixes=("_prev", ""),
                      indicator=True)
        df['group'] = np.where(
            ((df['timestamp'] > df['timestamp_prev']) &
            (df['_merge'] == 'both')), 'base', None)

        if (funnel_type == 'open'):
            df['group'] = np.where(
                (df['_merge'] == 'right_only') & (df['group'].isna()), 'new', df['group'])
            df['group'] = np.where(
                (df['_merge'] == 'both') & (df['group'].isna()), 'new', df['group'])
    step_counts = pd.pivot_table(data=df, index=pivot_list + ['group'], values='id',
                                 aggfunc=pd.Series.nunique).reset_index()

    return step_counts


if __name__ == '__main__':
    main()
