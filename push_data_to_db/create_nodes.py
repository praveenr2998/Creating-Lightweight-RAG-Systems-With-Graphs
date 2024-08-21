import pandas as pd
from neo4j import GraphDatabase
from openai import OpenAI

client = OpenAI(api_key="")
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


def get_embedding(text):
    """
    Used to generate embeddings using OpenAI embeddings model
    :param text: str - text that needs to be converted to embeddings
    :return: embedding
    """
    model = "text-embedding-3-small"
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model=model).data[0].embedding


def create_product_type(product_data_df):
    """
    Used to generate queries for creating product type nodes in neo4j
    :param product_data_df: pandas dataframe - data
    :return: query_list: list - list containing all create node queries for category
    """
    cat_query = """CREATE (a:Product_type {name: '%s', embedding: %s})"""
    distinct_product_types = product_data_df['Category'].unique()
    query_list = []
    for type_ in distinct_product_types:
        embedding = get_embedding(type_)
        query_list.append(cat_query % (type_, embedding))
    return query_list


def create_product_details(product_data_df):
    """
    Used to generate queries for creating product_details nodes in neo4j
    :param product_data_df: pandas dataframe - data
    :return: query_list: list - list containing all create node queries for product
    """
    product_query = """CREATE (a:Product_details {name: '%s', description: '%s', price: %d, warranty_period: %d, 
    available_stock: %d, review_rating: %f, product_release_date: date('%s'), embedding: %s})"""
    query_list = []
    for idx, row in product_data_df.iterrows():
        embedding = get_embedding(row['Product Name'] + " - " + row['Description'])
        query_list.append(product_query % (row['Product Name'], row['Description'], int(row['Price (INR)']),
                                           int(row['Warranty Period (Years)']), int(row['Stock']),
                                           float(row['Review Rating']), str(row['Product Release Date']), embedding))
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

# CREATE PRODUCT TYPE
query_list = create_product_type(product_data_df)
execute_bulk_query(query_list)

# CREATE PRODUCT DETAIL
query_list = create_product_details(product_data_df)
execute_bulk_query(query_list)

