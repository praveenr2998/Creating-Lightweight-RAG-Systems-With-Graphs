from neo4j import GraphDatabase
import pandas as pd

product_data_df = pd.read_csv('../data/product_data.csv')


def preprocessing(df, columns_to_replace):
    """
    Used to preprocess certain column in dataframe
    :param df: pandas dataframe - data
    :param columns_to_replace: list - column name list
    :return: df: pandas dataframe - processed data
    """
    df[columns_to_replace] = df[columns_to_replace].apply(lambda col: col.str.replace("'s", "s"))
    df[columns_to_replace] = df[columns_to_replace].apply(lambda col: col.str.replace("'", ""))
    return df


def create_type_detail_relationship_query(product_data_df):
    """
    Used to create relationship between Product_type and Product_details
    :param product_data_df: dataframe - data
    :return: query_list: list - cypher queries
    """
    query = """MATCH (c:Product_type {name: '%s'}), (p:Product_details {name: '%s'}) CREATE (c)-[:CONTAINS]->(p)"""
    query_list = []
    for idx, row in product_data_df.iterrows():
        query_list.append(query % (row['Category'], row['Product Name']))
    return query_list


def execute_bulk_query(query_list):
    """
    Executes queries is a list one by one
    :param query_list: list - list of cypher queries
    :return: None
    """
    url = "bolt://localhost:7687"
    auth = ("neo4j", "neo4j@123")

    with GraphDatabase.driver(url, auth=auth) as driver:
        with driver.session() as session:
            for query in query_list:
                try:
                    session.run(query)
                except Exception as error:
                    print(f"Error in executing query - {query}, Error - {error}")


# PREPROCESSING
product_data_df = preprocessing(product_data_df, ['Product Name', 'Description'])

# CATEGORY - FOOD RELATIONSHIP
query_list = create_type_detail_relationship_query(product_data_df)
execute_bulk_query(query_list)
