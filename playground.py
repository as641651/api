from pycelonis import *
from pycelonis.celonis_api.pql.pql import PQL
from pycelonis.celonis_api.pql.pql import PQLColumn
from pycelonis.celonis_api.pql.pql import PQLDebugger
from app import range_specifier

GLOBAL_VARS = {}

GLOBAL_VARS["celonis"] = get_celonis(
    url="https://academic-aravind-sankaran-rwth-aachen-de.eu-2.celonis.cloud",
    api_token="OWIyODI0ZTYtZDYyMS00N2QwLTk1ZTEtN2ZhN2U3NGYzYjg0Okp2a3FQeUNNanovVjM0K0ZHTVNpbTJPQWt1Y2RtZGdINlR2THRDbHd3Y1By"
)


def calc_throughput():
    # if GLOBAL_VARS["datamodel_name"] == "MobIS":
    query = PQL()
    string = f'CALC_THROUGHPUT ( {range_specifier("first", "pay expenses")} TO {range_specifier("all", "decide on travel expense approval")}, REMAP_TIMESTAMPS( "mobis_challenge_log_2019_csv"."END", MINUTES ) )'
    query += PQL(string)
    return GLOBAL_VARS["celonis"].datamodels.find("MobIS").get_data_frame(query)


print(f'CALC_THROUGHPUT ( {range_specifier("first", "pay expenses")} TO {range_specifier("all", "decide on travel expense approval")}, REMAP_TIMESTAMPS( "mobis_challenge_log_2019_csv"."END", MINUTES ) )')


newdatastore = calc_throughput()
print(newdatastore)

""" q = PQL()
q += PQLColumn('REMAP_TIMESTAMPS( "mobis_challenge_log_2019_csv"."END", MINUTES )')
GLOBAL_VARS["celonis"].datamodels.find(
        "a45a4f06-668e-461b-9c4a-80167ccdaf42").get_data_frame(q).to_csv("timestamp_minutes.csv") """

""" q = PQL()
q += PQLColumn(query='"mobis_challenge_log_2019_csv"."ACTIVITY"', name="case:concept:name")
GLOBAL_VARS["celonis"].datamodels.find("a45a4f06-668e-461b-9c4a-80167ccdaf42")._get_data_frame(q).to_csv("test.csv")  
 """

#datamodel = celonis.datamodels.find('')
