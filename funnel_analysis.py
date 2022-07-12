# import packages
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
from constants import *
from funnel_process_output import *

# Global variables

BQ_PROJECT = "bigquery-anwb"
BQ_DATASET_MASTER = "87223461"
BQ_DATASET_GA4_test = "analytics_290476086"
BQ_DATASET_GA4_prod = "analytics_229981733"

class ProcessFunnel:

    def __init__(self, definitions: dict, dryrun: bool = False):
        # self.settings = settings
        self.definitions = definitions['funnel'].to_dict()
        self.settings = {'definitions': self.definitions}
        self.prepped = {}
        self.prep_funnel()
        self.client = bigquery.Client(project=BQ_PROJECT)
        self.job_config = bigquery.QueryJobConfig()
        self.query_job = None
        self.query_bytes = 0
        self.cost = 0
        self.errors = []

    def set_funnel_scope(self):
        self.prepped['funnel_scope'] = f"""{self.lookup_bq_names(self.definitions['funnel_scope'])}"""

    def set_funnel_dimensions(self):
        # slechts 1 breakdown dimension toegestaan
        if self.definitions['breakdown_dimension']:
            self.prepped['breakdown_dimension'] = self.lookup_bq_names(self.definitions['breakdown_dimension'])
        else:
            self.prepped['breakdown_dimension'] = ['"-"']
        funnel_unnest = []
        for f in self.prepped['breakdown_dimension']:
            funnel_unnest += self.check_unnests(f)
        funnel_unnest += self.check_unnests(self.prepped['breakdown_dimension'])
        self.definitions['unnest'] = funnel_unnest

    def set_funnel_date_range(self):
        date_from = self.definitions['date_range'][0]
        date_to = self.definitions['date_range'][1]
        self.prepped['date_from'] = datetime.strptime(date_from, '%d-%m-%Y').date().strftime("%Y%m%d")
        self.prepped['date_to'] = datetime.strptime(date_to, '%d-%m-%Y').date().strftime("%Y%m%d")

    def set_funnel_filters(self):
        # self.prepped['filters'] = []
        # if self.definitions['breakdown_dimension']:
        #     filter = {}
        #     filter['dimension'] = self.lookup_bq_names(self.definitions['breakdown_dimension'])
        #     filter['condition'] = self.definitions['filter']
        #     filter['value'] = self.definitions['value']
        #     self.prepped['filters'].append(filter)
        self.prepped['filters'] = self.definitions['filters']
        for f in self.prepped['filters']:
            f['dimension'] = self.lookup_bq_names(f['dimension'])
            # funnel_filter_sql = self.process_filters(self.prepped['filters'])
            # self.prepped['funnelFilters'] = funnel_filter_sql
            self.prepped['funnel_filters'] = self.process_filters(f)

        funnel_filter_sql = [self.process_filters(f) for f in self.prepped['filters']]
        self.prepped['funnel_filters'] = "(" + """"
        AND    """.join(funnel_filter_sql) + ")"

    def set_funnel_steps(self):

        self.prepped['steps'] = []
        for step in self.definitions['steps']:
            sql_step_def = {'stepnumber': step['stepnumber']}
            s = []
            # voor iedere stap moeten zowel de benodigde unnest voor de hele funnel als de steps toegevoegd worden
            all_unnests = self.prepped['unnest']
            for f in step['step']:
                f['dimension'] = self.lookup_bq_names(f['dimension'])
                s.append(self.process_filters(f))
                step_sql = "(" + """"
        AND    """.join(s) +")"
                all_unnests += self.check_unnests(f['dimension'])

            sql_step_def['filters'] = step_sql

            unnest_dedup = list(set([item for subset_of_unnests in all_unnests for item in subset_of_unnests]))

            # unnest_dedup = list(dict.fromkeys(unnest))  # deduplicate list
            sql_step_def['unnest'] = """
                    , """.join(unnest_dedup)
            self.prepped['steps'].append(sql_step_def)
            # print(sql_step_def)

    def lookup_bq_names(self, ga_name: str):
        # translate ga dimensions and metrics to bq names
        # for translation list, see constants.py

        try:
            bq_name = GA_TO_BQ[ga_name.lower()]
        except KeyError:
            raise UserWarning(f'Variabele {ga_name} kan niet worden gebruikt in de query')

        return bq_name

    def process_filters(self, filter_):
        """"
        Transformeer de filterdefinities van dictionary naar sql formaat
        """
        # dimension, condition, value = filter_.values()
        dimension = filter_['dimension']
        condition = filter_['condition']
        value = filter_['value']
        if condition == 'regex':
            value = value.replace('\\', '\\\\')
            filter_sql = f"""REGEXP_CONTAINS({dimension},'{value}')"""
        elif condition == 'in':
            filter_sql = f"""{dimension} {condition} ({value})"""
        else:
            filter_sql = f"""{dimension} {condition} '{value}'"""
        return filter_sql

    def check_unnests(self, dimension):
        """"
        Bepaal welke items ge-unnest moeten worden
        """
        import re
        unnest_list = [unnest for regex, unnest in UNNEST_LOOKUP.items() if re.search(regex, dimension)]
        return unnest_list

    def prep_funnel(self):
        # self.definitions = funnel_definitions
        self.prepped = {'unnest': []}
        self.set_funnel_scope()
        self.set_funnel_dimensions()
        self.set_funnel_date_range()
        self.set_funnel_filters()
        self.set_funnel_steps()

        for index, step in enumerate(self.prepped['steps']):
            self.prepped['steps'][index] = self.prep_sql(step)
        self.settings['prepped'] = self.prepped
        return self.settings

    def prep_sql(self, step):
        sql = f"""-- Step {step['stepnumber']}
        CREATE TEMP FUNCTION customDimensionByIndexUA(indx INT64, arr ARRAY<STRUCT<index INT64, value STRING>>) AS (
        (   SELECT x.value FROM UNNEST(arr) x WHERE indx=x.index)
        );
        SELECT distinct {self.prepped['funnel_scope']} as id
            ,  visitStartTime + cast(hits.time/1000 as int64) as timestamp
            ,  EXTRACT(DATETIME FROM TIMESTAMP_SECONDS(visitStartTime + cast(hits.time/1000 as int64)) AT TIME ZONE "Europe/Amsterdam") as datetime
            ,  {self.prepped['breakdown_dimension']} as breakdown
        FROM   `{BQ_PROJECT}.{BQ_DATASET_MASTER}.ga_sessions_*` s, {step['unnest']}
        WHERE  _TABLE_SUFFIX >= '{self.prepped['date_from']}'
        AND    _TABLE_SUFFIX <= '{self.prepped['date_to']}'"""

        if self.prepped['funnel_filters']:
            sql += f"""
        AND    {self.prepped['funnel_filters']}"""

        if step['filters']:
            sql += f"""
        AND    {step['filters']}"""

        step['step_sql'] = sql
        return step

    # SQL statement from session data
    def get_UA_data(self, stepnumber, sql: str):
        print(sql)
        df = pd.read_csv(f'''{self.definitions['name']} {str(stepnumber)}.csv''', comment='#')

        # self.bq_call(sql)
        # bq_result = self.query_job.result()
        # df = bq_result.to_dataframe(create_bqstorage_client=True)
        #
        # self.calculate_query_bytes()
        #
        # df.to_csv(f'''{str(stepnumber)}.csv''', index=False)
        return df

    def calculate_query_bytes(self):
        if self.query_job.total_bytes_processed:
            self.query_bytes += self.query_job.total_bytes_processed

    def get_query_cost(self):
        self.cost = (self.query_bytes / 1099511627776) * 5
        return {'data': {
            'cost': self.cost,
            'errors': self.errors
        }}

    def dryrun(self):

        for step in self.prepped['steps']:

            self.job_config.dry_run = True
            self.job_config.use_query_cache = False

            self.bq_call(step['stepsql'])
            self.calculate_query_bytes()

        return self.get_query_cost()

    def bq_call(self, sql):
        self.query_job = self.client.query(sql, job_config=self.job_config)
        if self.query_job.errors:
            self.errors += self.query_job.errors

    def get_data(self):
        prev_stepnumber = None  # geen best practice om df zo op te bouwen, maar werkt wel
        step_dict = {}
        funnel_count = pd.DataFrame()

        prev_step = None
        for step in self.prepped['steps']:
            stepnumber = step['stepnumber']
            df_step = self.get_UA_data(stepnumber, step['step_sql'])
            step_dict[stepnumber] = df_step

            df_count = self.compare_steps(step_dict, stepnumber, prev_stepnumber)

            funnel_count = funnel_count.append(df_count, ignore_index=True)

            prev_stepnumber = stepnumber
        return funnel_count, self.get_query_cost()

    def compare_steps(self, step_dict, stepnumber, prev_stepnumber):

        # step_counts = {}
        step = step_dict[stepnumber].copy()

        # pivot_list = [i.split('.', 1)[-1] for i in breakdown_dimensions]
        if prev_stepnumber is None:
            df = step
            df['stepnumber'] = stepnumber
            df['group'] = 'base'
        else:
            prev_step = step_dict[prev_stepnumber]
            df = pd.merge(left=prev_step, right=step, how='outer',
                          on=['id', 'breakdown'],
                          suffixes=("_prev", ""),
                          indicator=True)
            df['stepnumber'] = stepnumber
            df['group'] = np.where(
                (df['timestamp'] > df['timestamp_prev']) &
                (df['_merge'] == 'both'), 'base', None)
            df = df.rename(columns={"_merge": "match"})
            # identify matched ids in base
            matched_id = df[df['group']=='base'][['id', 'breakdown']].drop_duplicates()
            df1 = pd.merge(df, matched_id, on=['id', 'breakdown'], how='left', indicator=True)
            df = df1[((df1['_merge'] == 'both') & (df1['group'].notnull())) |(df1['_merge'] == 'left_only')]\
                .drop(columns=['_merge'])
            # pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
            if self.definitions['funnel_type'] == 'open':
                df['group'] = np.where(
                    (df['match'] == 'right_only') & (df['group'].isna()), 'new', df['group'])
                df['group'] = np.where(
                    (df['match'] == 'both') & (df['group'].isna()), 'new', df['group'])
        step_counts = pd.pivot_table(data=df, index=['stepnumber', 'breakdown', 'group'], values='id', aggfunc=pd.Series.nunique).reset_index()

        return step_counts

def process():
    funnel_definitions = pd.read_json('funneldefinitions privatelease.json')
    funnel = ProcessFunnel(funnel_definitions)

    funnel_count, cost = funnel.get_data()
    if not funnel_count.empty:
        out = FunnelProcessOutput(funnel.settings, funnel_count)
        out.write_output_excel(funnel_count)
    return cost

#
# {"data": {
#   "cost": 1.23,
#   "errors": []
# }}
def dryrun():
    funnel_definitions = pd.read_json('funneldefinitions privatelease.json')
    funnel = ProcessFunnel(funnel_definitions)
    print(funnel.dryrun())

if __name__ == '__main__':
    # dryrun()

    cost = process()
    print(cost)
