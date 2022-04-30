from pycelonis import get_celonis
from pycelonis import pql
from app import *
import os
from dotenv import load_dotenv

celonis = get_celonis(
    url=("https://academic-b-berchiche-esi-dz.eu-2.celonis.cloud/"),
    api_token=("MDlhZjY4YWYtNjNiZC00MTg1LTg4YzUtYTRkZjkzZDE3ZDY1Ondrci9XYjA5Y2RTQXgwZk1mbHlvUkNmSHFTOVJNd0RydS9iZDFnaFB2cHRr"),
    key_type="USER_KEY"
)

""" # Function that takes a Celonis data model and uses the PQL query language to find all the data in the model then returns it as a list
def find_data(model):
    query = pql.Query(model)
    query.select("*")
    data = query.execute()
    return data
print(find_data("3630075d-34fc-4780-90b6-82f5372a7369")) """


def get_datamodel(id):
    """
    get_datamodel gets a datamodel from the celonis api and returns it.
    :param id: id of the datamodel
    :return: datamodel
    """
    return celonis.datamodels.find(id)


model = get_datamodel("3630075d-34fc-4780-90b6-82f5372a7369")


def get_table_names(model):
    """
    get_table_names gets the names of the tables in a model.
    :param model: datamodel from the celonis api
    :return: list of table names in the model
    """
    return [table.name for table in model.tables]


def get_tables(model):
    # get_tables takes in a datamodel and returns the tables in the model
    return model.tables


def get_columns_for_table(table):
    # get_columns_for_table takes in a datamodeltable and returns a list of columbs for the table
    return table.columns


def get_columns_for_model(model):
    """
    get_columns_for_model gets all columns from a celonis datamodel.
    :param model: datamodel from the celonis api
    :return: dictionary with table names as keys and lists of column names as values
    """
    columns = {}
    for table in model.tables:
        specific_table = [column['name'] for column in table.columns]
        columns[table.name] = specific_table
    return columns


def generate_query(table_name, column_name, points=2, radius=2):
    """
    generate_query generates a query for a specific table and column.
    :param table_name: name of a table in the datamodel
    :param column_name: name of a column in the table
    :return: query for the table and column
    """
    if radius < 0 or radius > 5:
        # Throw an error if the radius is not between 0 and 5
        raise ValueError("Radius must be between 0 and 5")

    p = pql.PQL()
    number_of_values = 1
    recursion_depth = 1

    # If recursion_depth or number_of_values is less than 1, throw an error
    if recursion_depth < 1 or number_of_values < 1:
        raise ValueError(
            "Recursion depth and number of values must be greater than 0")

    p += pql.PQL(
        f"ESTIMATE_CLUSTER_PARAMS ({table_name}.{column_name}, {radius}, {number_of_values}, {recursion_depth} )")

    q = pql.PQL()
    q += pql.PQLColumn(f"VARIANT({table_name}.{column_name})", "Variant")
    q += pql.PQLColumn(
        f"CLUSTER_VARIANTS( VARIANT({table_name}.{column_name}), {points}, {radius})", "Cluster")
    return q


def get_queries(model):
    """
    get_queries gets all possible queries for a celonis datamodel.
    :param model: datamodel from the celonis api
    :return: list of queries
    """
    queries = []
    for table in get_tables(model):
        queries.extend(generate_query(
            table.name, column['name']) for column in get_columns_for_table(table))

    return queries


def get_data(model):
    """
    get_data gets all data from a celonis datamodel and saves them as a csv.
    :param model: datamodel from the celonis api
    :return: none
    """
    data = []
    for query in get_queries(model):
        model.get_data_frame(query).to_csv(f"{query}.csv")


def cluster_all(table):
    query = pql.PQL()
    table_name = table.name
    for column in table.columns:
        if column["name"] == "ACTIVITY":
            query += pql.PQLColumn(
                query=f'{table_name}.{column["name"]}', name=f'case:{column["name"].lower()}')
            query += pql.PQLColumn(
                query=f'VARIANT( {table_name}.{column["name"]} )', name='case:variant')
            query += pql.PQLColumn(query=f'CLUSTER_VARIANTS ( VARIANT (  {table_name}.{column["name"]} ) , 2 , 2 )',
                                   name="case:cluster_id")
        elif column["name"] == "CASE":
            query += pql.PQLColumn(query=f'"{table_name}"."{column["name"]}"',
                       name="case:concept:name")
        else: query += pql.PQLColumn(
            query=f'"{table_name}"."{column["name"]}"', name=f'{column["name"]}')
    return query


def cluster_variants():
    query = pql.PQL()
    query += pql.PQLColumn(query='"mobis_challenge_log_2019_csv"."CASE"',
                           name="case:concept:name")
    query += pql.PQLColumn(query='"mobis_challenge_log_2019_csv"."ACTIVITY"',
                           name="concept:name")
    query += pql.PQLColumn(query='"mobis_challenge_log_2019_csv"."START"',
                           name="timestamp")
    query += pql.PQLColumn(query=' VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" )',
                           name="case:variant")
    query += pql.PQLColumn(query='CLUSTER_VARIANTS ( VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) , 40 , 2 )',
                           name="case:cluster_id")
    print(query)
    datamodel = celonis.datamodels.find("MobIS")
    datamodel.get_data_frame(query).to_csv("cluster_all.csv")


""" #q = pql.PQL()
#q += pql.PQL(f"{get_table_names(model)[0]}.{get_columns_for_table(get_tables(model)[0])[2]['name']}")
#q += pql.PQL(f"SELECT * from {get_tables(model)[0]};")

q += pql.PQLColumn(
    f"VARIANT({get_table_names(model)[0]}.{get_columns_for_table(get_tables(model)[0])[2]['name']})", "Variant")
q += pql.PQLColumn(
    f"CLUSTER_VARIANTS( VARIANT({get_table_names(model)[0]}.{get_columns_for_table(get_tables(model)[0])[2]['name']}), 2, 1)", "Cluster") 

for table in get_tables(model):
    table.get_data_frame().to_json(f"{table.name}.json")
#df = model.get_data_frame(q)
# df.to_json("test.json")
# df.to_csv("clusterized_test.csv") """
