import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, asc, desc

def create_spark_session(appName, config =None):
    """Create a Spark session named as appName and return the session object"""
    """_summary_
        appName(str) : The name of spark application.
        config(any) : config mode
    Returns:
        spark_session: a spark session will be create or get (if exist)
    """
    # Initialize FindSpark to get hold of Spark Session
    findspark.init()
    if config  is None:
    # Create a Spark session, set app name and mode (in this case it's 'local')
        spark = SparkSession\
            .builder\
            .appName(appName)\
            .getOrCreate()
    else:
        spark = SparkSession\
            .builder\
            .appName(appName)\
            .config(**config)\
            .getOrCreate()    
    print("Running spark version: ", spark.version )   
    return spark

def read_data(spark_session, file_path, mode):
    """_summary_

    Args:
        file_path (string): a path  to the data file
        mode (string): data file type they are csv or json files
    Returns:
        a dataframe  which is result of reading the given file in the specified format
    """
    
    if  mode == "csv":
        df = spark_session.read.csv(file_path)
    elif mode == "json":
        df = spark_session.read.json(file_path)
    else:
        print("Unsupported File Type!")
        df=None
    return df 

def write_data(df, output_path, mode="parquet", overwrite=True):
    """Save the DataFrame `df` into an external storage system such as HDFS, S3, NFS, etc.
       The data will be saved in the Parquet format by default. If you want to save it in other formats like JSON/CSV
    """
    """_summary_
        df(dataframe): original dataframe
        output_path(str): the location where you want to save your data
        mode(str): the format of the  saved file parquet/csv/json..etc
        overwrite(bool): should we overwrite dataframe
    """
    df.write.mode('overwrite' if overwrite else 'append').format(mode).save(output_path)

def show_data(df, limit_lines, mode='pandas'):
    """Print Schema and first n row of the DataFrame `df`.
    """
    """_summary_

    Args:
        df (dataframe): original data frame
        limit_lines (int): number of lines displayed in pandas mode
        mode (str, optional): 'pandas' or 'spark'. Defaults to 'pandas'.
    """    
    
    if mode == 'pandas':
        df.limit(limit_lines).toPandas()
    else: #normal mode
        df.show(limit_lines)
       
def print_schema(df):
    """Print schema of the DataFrame `df`.
    """
    df.printSchema() 

# def create_table(spark_session, table_name, select_statement,  database_name=None, save_as_table=False):
#     """Create a new Spark temporary view or a permanent table based on the SQL query expression
#     Args:
#         spark_session : A spark session object
#         table_name : The name of the table that will be created
#         select_statement : The SQL statement  used to retrieve the data for this table
#         database_name : If provided it creates the table inside the specific database otherwise default one
#         save_as_table : Whether we want to save the result of the select statement as a table or not
#     """
#     if database_name is None:
#         df = spark_session.sql(select_statement)
#     else:
#         df = spark_session.sql(f"SELECT * FROM {database_name }.{select_statement}")

#     if save_as_table :
#         df.createOrReplaceTempView(table_name)
#     else: 
#         df.createOrReplaceGlobalTempView(table_name) 
# @contextmanager
# def add_tmp_column(spark, df, column_name, value):
#     """A context manager that adds a temporary column with constant value to dataframe"""
#     from pyspark.sql import functions as F
#     try:
#         df.withColumn(column_name,F.lit(value)).show()
#         yield df.withColumn(column_name,F.lit(value))
#     finally:
#         df.drop(column_name).show()    
    
def show_selected_cols(df, cols, limit_lines, mode='pandas'):
    """_summary_

    Args:
        df (dataframe): original data frame
        cols (Column list): a list of columns to  display
        limit_lines (int): number of lines displayed in pandas mode
        mode (str, optional): 'pandas' or 'spark'. Defaults to 'pandas'.
        
    """
    selected_df = df.select(cols)
    show_data(selected_df, limit_lines, mode)
    
def change_data_type(df, col, new_type, date_format = None):
    """_summary_

    Args:
        df (dataframe): original dataframe
        col (ColumnOrName): column need to change data type
        new_type (pyspark.sql.types or str): new type of data to change
        date_format (str, optional): Defaults to None. 
        if  `new_type` is 'date' then provide the format of the date. Example "yyyy-MM-dd".

    Returns:
        dataframe: _description_
    """
    if new_type != 'date':
        return df.withColumn(col, df[col].cast(new_type))
    else:
        return df.withColumn('last_review', to_date(df[col], format=date_format))
        

#def get_dtypes(df):
#    return dict([(c.name, c.dataType) for c in df.schema])

#def rename_columns(df, old_names, new_names):
#    assert len(old_names) == len(new_names), "Number of old names must match number of new names"
#    return df.toDF(*(new_names + [x for x in df.columns if x not in old_names]))

#def drop_columns(df, columns):
#    return df.drop(*columns)

def group_by(df, cols):
    grouped_df =  df.groupBy(cols)
    return grouped_df # grouped_df will need a statatiic  method like agg() or sum() to be useful

def order_by(df, col, order):
    if  order.lower() == 'asc':
        return df.orderBy(asc(col))
    elif order.lower()== 'desc':
        return df.orderBy(desc(col))
    else:
        raise ValueError("Order parameter should be either asc or desc")
    
