from flask import Flask, jsonify, request
from flask_cors import CORS
#import jsonify

app = Flask(__name__)
CORS(app)

from pycelonis import get_celonis
from pycelonis.celonis_api.pql.pql import PQLColumn
from pycelonis.celonis_api.pql.pql import PQL

import utils

import pm4py
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.statistics.traces.generic.log import case_statistics

GLOBAL_VARS = {}

GLOBAL_VARS["celonis"] = get_celonis(
    url="https://academic-aravind-sankaran-rwth-aachen-de.eu-2.celonis.cloud",
    api_token = "OWIyODI0ZTYtZDYyMS00N2QwLTk1ZTEtN2ZhN2U3NGYzYjg0Okp2a3FQeUNNanovVjM0K0ZHTVNpbTJPQWt1Y2RtZGdINlR2THRDbHd3Y1By"
)


@app.route("/loaddata")
def load_data():
    #http://127.0.0.1:5000?name=aravind
    name = request.args.get("name")
    GLOBAL_VARS["datamodel_name"] = name
    GLOBAL_VARS["datamodel"] = GLOBAL_VARS["celonis"].datamodels.find(name)
    response = jsonify(data="success")
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route("/cluster")
def cluster_variants():
    if GLOBAL_VARS["datamodel_name"] == "MobIS":
        #estimate_cluster_params();
        query = PQL()
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."CASE"', name="case:concept:name")
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."ACTIVITY"', name="concept:name")
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."START"', name="timestamp")
        query += PQLColumn(query=' VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" )', name="case:variant")
        query += PQLColumn(query='CLUSTER_VARIANTS ( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) , 40 ,3 )',
                           name="case:cluster_id")
        # query += PQLColumn(query='CLUSTER_VARIANTS ( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) ,  , 2 )',
        #                     name="case:cluster_id")
        #query += PQLColumn(query=f'CLUSTER_VARIANTS ( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) , {GLOBAL_VARS["cluster_params"]} , 4 )',
        #                   name="case:cluster_id")
        GLOBAL_VARS["dataframe"] = GLOBAL_VARS["datamodel"]._get_data_frame(query)
        response = jsonify(data="success mobis")

    if GLOBAL_VARS["datamodel_name"] == "SAP P2P":
        query = PQL()
        query += PQLColumn(query='"_CEL_P2P_ACTIVITIES_EN_parquet"."_CASE_KEY"', name="case:concept:name")
        query += PQLColumn(query='"_CEL_P2P_ACTIVITIES_EN_parquet"."ACTIVITY_EN"', name="concept:name")
        query += PQLColumn(query='"_CEL_P2P_ACTIVITIES_EN_parquet"."EVENTTIME"', name="timestamp")
        query += PQLColumn(query=' VARIANT ( "_CEL_P2P_ACTIVITIES_EN_parquet"."ACTIVITY_EN" )', name="case:variant")
        query += PQLColumn(
            query='CLUSTER_VARIANTS ( VARIANT ( "_CEL_P2P_ACTIVITIES_EN_parquet"."ACTIVITY_EN" ) , 40 , 2 )',
            name="case:cluster_id")
        #print(query)
        #GLOBAL_VARS["dataframe"] = GLOBAL_VARS["datamodel"]._get_data_frame(query)
        GLOBAL_VARS["dataframe"] = utils.get_sap_p2p_df()
        response = jsonify(data="success p2p")

    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route("/tracecounts")
def get_trace_counts():
        log = utils.convert_df_to_log(GLOBAL_VARS["dataframe"])
        GLOBAL_VARS["variants_count"] = utils.get_variants_count(log)
        response = jsonify(data = GLOBAL_VARS["variants_count"])
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

@app.route("/variantstable")
def get_variants_table():
    GLOBAL_VARS["variant_table"] = utils.get_variants_table(GLOBAL_VARS["dataframe"])
    json_data = GLOBAL_VARS["variant_table"].to_json(orient='index')
    response = {'data': json_data}
    #response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route("/topclustersactivities")
def get_top_clusters_activities():
    top_clusters_activities, GLOBAL_VARS['num_clusters'] = utils.get_top_clusters_activities(GLOBAL_VARS["variant_table"])
    response = jsonify(data=top_clusters_activities)
    return response

@app.route("/numclusters")
def get_num_clusters():
    response = jsonify(data=GLOBAL_VARS['num_clusters'])
    return response

@app.route("/activitiescounts")
def get_activities_count():
        log = utils.convert_df_to_log(GLOBAL_VARS["dataframe"])
        GLOBAL_VARS["activities_count"] = utils.get_activities_count(log)
        response = jsonify(data = GLOBAL_VARS["activities_count"])
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

def estimate_cluster_params():
    query = PQL()
    query += PQLColumn(
        query='ESTIMATE_CLUSTER_PARAMS( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ), 2, 10, 10 )')

    GLOBAL_VARS["cluster_params"] = GLOBAL_VARS["datamodel"]._get_data_frame(query)
    print(GLOBAL_VARS["cluster_params"])
    #469,280,203,132

    #exit(-1)

@app.route("/throughput")
def get_cluster_throughput():

    #print(request.args.get("clusters"))
    #exit(-1)

    cluster_ids = list(map(int, request.args.get("clusters").split(",")))

    if GLOBAL_VARS["datamodel_name"] == "MobIS":
        data = get_throughput_mobis(cluster_ids)
    if GLOBAL_VARS["datamodel_name"] == "SAP P2P":
        data = get_throughput_csv(cluster_ids)

    #print(data)
    #exit(-1)

    response = jsonify(data=data)
    return response

def get_throughput_csv(cluster_ids):
    throughput_df = pd.read_csv('data/sap_throughput.csv', sep=',')

    throughput_df['avg_throughput_time'] = throughput_df['avg_throughput_time'].apply(lambda x: x / 60. / 24.)

    GLOBAL_VARS["cluster_throughputs"] = throughput_df

    data = []
    for cluster in cluster_ids:
        vals = throughput_df.loc[throughput_df['cluster'] == cluster].values
        data.append([int(vals[0][0]), int(vals[0][1])])

    return data

def get_throughput_mobis(cluster_ids):


    activity_table_activity = '"mobis_challenge_log_2019_csv"."ACTIVITY"'
    activity_table_timestamp = '"mobis_challenge_log_2019_csv"."END"'

    throughput_per_cluster = PQL()
    throughput_per_cluster += PQLColumn(
        f'CLUSTER_VARIANTS ( VARIANT ( {activity_table_activity} ), 40, 3 ) ', "cluster"
    )
    throughput_per_cluster += PQLColumn(
        f'AVG ('
        f'  CALC_THROUGHPUT ( '
        f'      CASE_START TO CASE_END, '
        f'      REMAP_TIMESTAMPS ( {activity_table_timestamp}, MINUTES ) '
        f'  ) '
        f')',
        "avg_throughput_time"
    )

    throughput_df = GLOBAL_VARS["datamodel"]._get_data_frame(throughput_per_cluster)

    GLOBAL_VARS["cluster_throughputs"] = throughput_df

    throughput_df['avg_throughput_time'] = throughput_df['avg_throughput_time'].apply(lambda x: x / 60. / 24.)

    data = []
    for cluster in cluster_ids:
        vals = throughput_df.loc[throughput_df['cluster'] == cluster].values
        data.append([int(vals[0][0]), int(vals[0][1])])

    return data

@app.route("/throughput-all")
def get_cluster_throughput_all():
    df = GLOBAL_VARS["cluster_throughputs"]
    df = df.loc[df['cluster'] != -1].set_index('cluster')
    json_data = df.to_json(orient='index')
    response = {'data': json_data}
    return response


if __name__ == '__main__':

    app.run(port=5003,debug=True)