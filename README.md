# A data analytics lecture using python and pyspark

Coursebook: python-data-analysis-3rd-edition.pdf

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Mid-term assignment: Data visualization

Reference: chapter 5 of the coursebook

Dataset to do experiments: choose any dataset if you want

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Content of the course:

1. Lession 1: Install pyspark and setup spark environment on Anaconda
   References:
   
   Pyspark on Anaconda: https://sparkbyexamples.com/pyspark/install-pyspark-in-anaconda-jupyter-notebook/?fbclid=IwAR3RNBGx4c6eKrxLTK9OP6PLX4TXflgIXbw9QYiYL8Icnrw6lb3Cc81dGNU

   Spark installation on Windows: https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/?fbclid=IwAR1k0Qu9ggAWIBkSRU9Q33pDCpp3nG8HQtoUPKnvK0NvilIj8ntP7IdKtvo
   
   
2. Lession 2: Read, Write and validate data for csv file and json file

   Create spark session
   ```sh
   import findspark
   findspark.init()
   import pyspark
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName('csvReader').getOrCreate()
   ```
   Read data
   ```sh
   path = 'dataset' #dataset path based on your definition
   # read csv file
   students = spark.read.csv(path+'students.csv', inferSchema = True, header = True)
   # inferSchema and header parameter to define first line of csv file is header and schema of dataframe
   # read json file
   people = spark.read.json(path+'people.json')
   ```
   Define data structure
   ```sh
   from pyspark.sql.types import StructField, StringType, DateType, StructType
   list_schema =[StructField("name", StringType(), True),
              StructField("timestamp", DateType(), True)]
   # read json data file again to read defined structure
   data_struct = StructType(fields=list_schema)
   people = spark.read.json(path+'people.json', schema=data_struct)
   ```
   - ex1.ipynb for csv data
   - ex2.ipynb for json data
   - dataset for homework: https://drive.google.com/file/d/1bw7pEgXSVLyMuaI_s3FPa5smNKHsu7-c/view?fbclid=IwAR1XUrTk0Oj0k26f2mS889ZkQEGx3FCI4i7rdO3zNVi5ZM-DpahqUCX8aN4
   - homework (completed): exercises 1, 2, 3, 4, 5, 10 at Read_Write_and_Validate_HW.ipynb 

3. Lesson 3: Data manipulation
   
   Learn about datatypes by pyspark
   ```sh
   #import modules
   import pyspark.sql.types
   #or
   from pyspark.sql.types import *
   #some classes are available for pyspark.sql.types module
   #DataType
   #NullType
   #StringType
   #BinaryType
   #BooleanType
   #DateType
   #TimestampType
   #DecimalType
   #DoubleType
   #FloatType
   #ByteType
   #IntegerType
   #LongType
   #ShortType
   #ArrayType
   #MapType
   #StructField
   #StructType
   ```
   How to change datatype of a column
   ```sh
   #using cast method from pyspark.sql.functions module
   from pyspark.sql.functions import cast
   #how to call a column indataframe
   #for example if we have a dataframe df and a column called views
   df.views
   #or
   from pyspark.sql.functions import col
   col('views')
   #for example if views column has string type and we want to change it into integer type
   from pyspark.sql.functions import col, cast
   from pyspark.sql.types import IntegerType
   df.withColumn('views', col('views').cast(IntegerType()))
   #or
   df.withColumn('views', df.views.cast(IntegerType()))
   ```
   - dataset for this lesson: https://www.kaggle.com/datasets/datasnaek/youtube-new?fbclid=IwAR1GafFaK6Pm1-voK-LRwJGG8Lgk1QWEd09UE661dVNZcAHqfnR_5-4ybN8#USvideos.csv
   - ex2-1.ipynb for lession 1 revision
   - ex2-2.ipynb for Youtube trending dataset from above URL 


4. Lesson 4: Data manipulation (cont)
   
   Regular expression (regex)

   Reference: https://digitalfortress.tech/tips/top-15-commonly-used-regex/?fbclid=IwAR3eqN04hlb5oz6V1Wn-hVgUvbQjsYFoASU_GO2jCdgBuKo7aowAN2MeVKw

   ```sh
   #for example
   # regex for a https URL:
   regex = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
   ```
   Assignments files:
   - Question 1-6(completed): Manipulating_Data_in_DataFrames_HW_Q1-6.ipynb
   - Question 7-9(completed): Manipulating_Data_in_DataFrames_HW_Q7-9.ipynb
   Dataset file for assignments
     ```sh
     #dataset for assignment question 1-6
     dataset_path = dataset\TweetHandle\ExtractedTweets.csv
     ```
   regexp_extract method

   Reference: https://docs.google.com/document/d/1gdoh50voz5iOsCS9Rd-JtNa9CCErQVkq/edit?fbclid=IwAR1Jl6usSspEyRfqmCzdYAsxdbulLv_9XzLfduVXYJCO8oU2OsrrPhdKn6k#heading=h.fvb53qjbsunw

   Example
   ```sh
   from pyspark.sql.functions import regexp_extract, col

   pattern = '(.)(@LatinoLeader)(.)'
   df = data.withColumn('Latino_mentions', regexp_extract(col('Tweet'), pattern,2))
   ```
   regexp_replace method

   Example
   ```sh
   from pyspark.sql.functions import regexp_replace
   pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'  # Regular expression pattern to match URLs
   clean = data.withColumn('clean', regexp_replace(data.Tweet, pattern, '')).select('clean')
   clean.show(truncate = False)
   ```
5. Lesson 5: Search and filter dataframe
   
   Dataset path for this lession
   ```sh
   path = 'dataset/fifa19.csv'
   ```
   Groupby method:
   ```sh
   #groupBy command need to use together with a statistic  function.
   #Here, I will show you how to calculate the mean and standard deviation of each group using 'groupBy' and 'apply'.
   #Here, we will use the groupBy

   group_df = df.groupBy("Nationality").count()
   group_df.show()
   ```
   Orderby method
   ```sh
   from pyspark.sql.functions import desc, asc
   df.orderBy(desc("Age")).show()
   print('##################################################################################################################################')
   df.orderBy(asc("Age")).show()
   ```
   Select with where clause method
   ```sh
   #select data by condition using where  clause
   fifa_df.select("Name", "Club").where(fifa_df.Club.like("%Barcelona%")).show(truncate=False)
   ```
   substr method
   ```sh
   photo_ext = fifa_df.select("Photo", fifa_df.Photo.substr(-3,3).alias("File_extension"))
   photo_ext.show()
   ```
   isin, startswith, endswith methods
   ```sh
   #df1 = fifa_df.Club.isin("Barcelona", "Juventus")
   df1 = fifa_df.select("Name", "Club").where(fifa_df.Club.isin(["Barcelona", "Juventus"]))
   #df2 = fifa_df.Name.startswith("B").where(fifa_df.Name.endswith("a"))
   df2 = fifa_df.select("Name").where(fifa_df.Name.startswith("B")).where(fifa_df.Name.endswith("a"))
   ```
   filter method
   ```sh
   filtered_df = fifa_df.filter(F.col("Name").isin(["L. Messi", "Cristiano Ronaldo"]))
   ```
   Homework (completed): Search and Filter DataFrames in PySpark-HW.ipynb file
6. Lesson 6: Aggregation dataframe
   
   Dataset for this lession
   ```sh
   dataset_path = 'dataset/nyc_air_bnb.csv'
   ```
   Assignment: Aggregating_DataFrames_in_PySpark_HW.ipynb file

   agg method
   ```sh
   grouped_df = airbnb.groupBy("host_id")

   # Calculate the total number of reviews for each host using 'count'
   total_reviews_per_host = grouped_df.agg(count("number_of_reviews").alias("total_reviews"))
   ```
   Differences between agg and summary method
   - agg to make statistics functions on a column
   - summary to make statistics functions on whole dataframe
   - Reference: https://docs.google.com/document/d/1Yc8x1z35s85CVD9MD7Dzq1g3H5DhkXtX/edit?fbclid=IwAR0uhm3RTzgNFhyKLdjZYQP6f5H2ACVPIa8DnrtrqFBnl7vnQjR40CHQT88#heading=h.fvb53qjbsunw
  
    pivot function

   Reference: https://docs.google.com/document/d/1F3ZnN2jInhvVqOu-EiWrHOKm3f3vDaXC/edit?fbclid=IwAR2a03HQWzYVF_fsnBuZLD7-wvpEZ_G3S6Os51wyLvPK2SLL8aujkfYVz9Y#heading=h.fvb53qjbsunw
   
   pivot example:
   ```sh
   from pyspark.sql.functions import col, avg, round

   # Filter for private and shared room types (replace with actual names if different)
   filtered_df = airbnb.filter(col("room_type").isin(["Private room", "Shared room"]))

   # Filter for Manhattan and Brooklyn only (replace with actual names if different)
   filtered_df = filtered_df.filter(
    col("neighbourhood_group").isin(["Manhattan", "Brooklyn"])
   )

   # List the room types you want to include (avoid duplicates)
   room_types_list = ["Private room", "Shared room"]

   # Use pivot to create a two-by-two table
   avg_price_per_listing = filtered_df.groupBy("neighbourhood_group") \
    .pivot("room_type", room_types_list) \
    .agg(round(avg("price"), 2).alias("avg_price"))

   # Display the results
   avg_price_per_listing.show()
   ```
8. Lesson 7: Joining and Appending dataframe

   SQL Join
   
   Reference: https://blog.codinghorror.com/a-visual-explanation-of-sql-joins/
   dataset for this lesson: 
