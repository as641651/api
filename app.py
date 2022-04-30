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
        query = PQL()
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."CASE"', name="case:concept:name")
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."ACTIVITY"', name="concept:name")
        query += PQLColumn(query='"mobis_challenge_log_2019_csv"."START"', name="timestamp")
        query += PQLColumn(query=' VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" )', name="case:variant")
        query += PQLColumn(query='CLUSTER_VARIANTS ( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) , 40 , 2 )',
                           name="case:cluster_id")

        GLOBAL_VARS["dataframe"] = GLOBAL_VARS["datamodel"]._get_data_frame(query)
        response = jsonify(data="success")
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


@app.route("/activitiescounts")
def get_activities_count():
        log = utils.convert_df_to_log(GLOBAL_VARS["dataframe"])
        GLOBAL_VARS["activities_count"] = utils.get_activities_count(log)
        response = jsonify(data = GLOBAL_VARS["activities_count"])
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

if __name__ == '__main__':

    app.run(port=5003, debug=True)