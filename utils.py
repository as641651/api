from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.statistics.traces.generic.log import case_statistics
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
import pandas as pd

def convert_df_to_log(df):
    log_csv = dataframe_utils.convert_timestamp_columns_in_df(df)
    log_csv = log_csv.sort_values('timestamp')
    log = log_converter.apply(log_csv)
    return log

def get_variants_count(log):
    variants_count = case_statistics.get_variant_statistics(log)
    variants_count = sorted(variants_count, key=lambda x: x['count'], reverse=True)
    for variant in variants_count:
        #variant['cluster'] = variants_cluster.loc[variants_cluster['case:variant'] == variant['variant']]['case:cluster_id'].values
        variant['variant'] = variant['variant'].split(",")
    return variants_count

def get_variants_table(df):
    df = df[['case:concept:name', 'case:variant', 'case:cluster_id']]
    df_pivot = df.pivot_table(columns=['case:variant'], aggfunc='size').reset_index().rename(columns={0: 'count'})
    merged_df = pd.merge(df, df_pivot, on='case:variant')
    merged_df = merged_df.drop_duplicates(subset=['case:variant']).set_index('case:concept:name')
    merged_df['case:variant'] = merged_df['case:variant'].apply(lambda x: x.split(","))
    return merged_df


def get_activities_count(log):
    variants_count = case_statistics.get_variant_statistics(log)
    variants_count = sorted(variants_count, key=lambda x: x['count'], reverse=True)
    activities_count = {}
    for variant in variants_count:
        #print(variant)
        for a in variant['variant'].split(","):
            try:
                activities_count[a] += variant['count']
            except KeyError:
                activities_count[a] = variant['count']
    return activities_count

