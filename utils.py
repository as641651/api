from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.statistics.traces.generic.log import case_statistics
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
import pandas as pd
import operator

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

def get_activity_counts_pd(variants_count_table):
    activities_count = {}
    for index, row in variants_count_table.iterrows():
        activities = row['case:variant']
        for a in activities:
            try:
                activities_count[a] += row['count']
            except KeyError:
                activities_count[a] = row['count']

    return activities_count


def get_top_clusters_activities(variants_table):
    cluster_ids = variants_table['case:cluster_id'].unique()
    cid = {}
    cid_df = []
    for i in cluster_ids:
        cid[i] = len(variants_table.loc[variants_table['case:cluster_id'] == i])
        cid_df.append([i, cid[i]])
    cid_df = pd.DataFrame(cid_df)
    top_c_df = cid_df.sort_values(by=[1], ascending=False).head()
    top_vaf_dfs = {}
    top_activities = {}
    for i in top_c_df[0].values:
        top_vaf_dfs[i] = variants_table.loc[variants_table['case:cluster_id'] == i]
        activity_counts = get_activity_counts_pd(top_vaf_dfs[i])
        top_activities[str(i)] = {
            'activities': sorted(activity_counts.items(), key=operator.itemgetter(1), reverse=True),
            'count': cid[i],
        }

    return top_activities, len(cluster_ids)-1

def get_sap_p2p_df():
    return pd.read_csv('data/sap_p2p.csv', sep=',')
