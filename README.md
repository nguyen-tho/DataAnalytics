A data analytics lecture using python and pyspark
Coursebook: python-data-analysis-3rd-edition.pdf
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
