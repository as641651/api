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


def minimal_cluster(table):
    """
    minimal_cluster returns a minimal version query that clusters all the data in a table with the variant being the activity
    :param table: datamodeltable
    :return: query
    """
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
    return query


def cluster_all(table):
    """
    cluster_all returns a query that clusters all the data in a table with the variant being the activity
    :param table: datamodeltable
    :return: query
    """
    query = minimal_cluster(table)
    table_name = table.name
    for column in table.columns:
        if column["name"] not in ["ACTIVITY", "CASE"]:
            query += pql.PQLColumn(
                query=f'"{table_name}"."{column["name"]}"', name=f'{column["name"]}')
    return query


def range_specifier(range, activity=""):
    """
    range_specifier returns a query that specifies a range of values for a specific activity
    :param range: range of values (enum of "all", "end", "first", or"last" 
    :param activity: activity to specify the range for
    return: query for the range/activity
    """
    if range == "all":
        return f"ALL_OCCURRENCES ['{activity}']"
    elif range == "end":
        return "CASE_END"
    elif range == "first":
        return f"FIRST_OCCURRENCE ['{activity}']"
    elif range == "last":
        return f"LAST_OCCURRENCE ['{activity}']"
    elif range == "start":
        return "CASE_START"


def calc_throughput(table, query, range_start=range_specifier("start"), range_end=range_specifier("end")):
    """
    calc_throughput calculates the throughput of a specific query for a specific table
    :param table: datamodeltable
    :param query: query to add the throughput filter to
    :param range_start: start of the range
    :param range_end: end of the range
    :return: throughput of the query
    """
    query += pql.PQLFilter(f"""CALC_THROUGHPUT ( {range_start} TO {range_end}, REMAP_TIMESTAMPS ( "{table.name}"."END", MINUTES ) ) = CALC_THROUGHPUT ( {range_start} TO {range_end}, REMAP_TIMESTAMPS ( "{table.name}"."END", MINUTES ) ) """)
    print(query)
    return query


def add_throughput(table, query):
    """
    add_throughput adds the throughput filter to a query
    :param table: datamodeltable
    :param query: query to add the throughput filter to
    :return: query with the throughput filter
    """
    query += pql.PQLColumn(
        query=f"""DATEDIFF ( hh , "{table.name}"."START" , "{table.name}"."END" )""", name="case:throughput")
    return query


def minimal_cluster_and_throughput(table):
    """
    minimal_cluster_and_throughput returns a minimal version query that clusters all the data in a table with the variant being the activity and adds the throughput filter
    :param table: datamodeltable
    :return: query
    """
    query = minimal_cluster(table)
    query = add_throughput(table, query)
    return query


""" query = minimal_cluster_and_throughput(
    celonis.datamodels.find("MobIS").tables[0])
celonis.datamodels.find("MobIS").get_data_frame(query).to_csv("filtered.csv") """


def throughput_per_cluster(table):
    """
    throughput_per_cluster returns a query that calculates the throughput per cluster for a specific table
    :param table: datamodeltable
    :return: query
    """
    query = pql.PQL()
    query += pql.PQLColumn(
        f'CLUSTER_VARIANTS ( VARIANT ( {table.name}."ACTIVITY" ), 2, 2 ) ', "cluster"
    )
    query += pql.PQLColumn(
        f'AVG ('
        f'  CALC_THROUGHPUT ( '
        f'      CASE_START TO CASE_END, '
        f'      REMAP_TIMESTAMPS ( {table.name}."START", MINUTES ) '
        f'  ) '
        f')',
        "avg_throughput_time"
    )
    return query


def get_cases_for_cluster(activity_table, cluster_id, case_table=""):
    """
    get_cases_for_cluster returns a query that gets all cases for a specific cluster
    :param activity_table: datamodeltable
    :param cluster_id: cluster id
    :param case_table: datamodeltable
    :return: query
    """
    query = pql.PQL()
    if case_table != "":
        query += pql.PQLColumn(""" "{case_table}"."CASE" """, 'case_id')
    query += pql.PQLColumn(
        f'VARIANT ( {activity_table}."ACTIVITY" )', "variant")
    query += pql.PQLFilter(
        f'FILTER CLUSTER_VARIANTS ( VARIANT ( {activity_table}."ACTIVITY" ), 3, 3 ) = {cluster_id}')


""" query = calc_throughput(celonis.datamodels.find(
    "MobIS").tables[0], minimal_cluster
(celonis.datamodels.find("MobIS").tables[0]))
celonis.datamodels.find("MobIS").get_data_frame(query).to_csv("filtered.csv") """
