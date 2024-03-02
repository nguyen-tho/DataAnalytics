A data analytics lecture using pyspark
1. Lession 1: Install pyspark and setup spark environment on Anaconda
   References:
   
   Pyspark on Anaconda: https://sparkbyexamples.com/pyspark/install-pyspark-in-anaconda-jupyter-notebook/?fbclid=IwAR3RNBGx4c6eKrxLTK9OP6PLX4TXflgIXbw9QYiYL8Icnrw6lb3Cc81dGNU

   Spark installation on Windows: https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/?fbclid=IwAR1k0Qu9ggAWIBkSRU9Q33pDCpp3nG8HQtoUPKnvK0NvilIj8ntP7IdKtvo
   
   
3. Lession 2: Read, Write and validate data for csv file and json file
   
   - ex1.ipynb for csv data
   - ex2.ipynb for json data
   - dataset for homework: https://drive.google.com/file/d/1bw7pEgXSVLyMuaI_s3FPa5smNKHsu7-c/view?fbclid=IwAR1XUrTk0Oj0k26f2mS889ZkQEGx3FCI4i7rdO3zNVi5ZM-DpahqUCX8aN4
   - homework: exercises 1, 2, 3, 4, 5, 10 at Read_Write_and_Validate_HW.ipynb

4. Lesson 3: Data manipulation
   
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


5. Lesson 4: Data manipulation (cont)
   
   Regular expression (regex)

   Reference: https://digitalfortress.tech/tips/top-15-commonly-used-regex/?fbclid=IwAR3eqN04hlb5oz6V1Wn-hVgUvbQjsYFoASU_GO2jCdgBuKo7aowAN2MeVKw

   ```sh
   #for example
   # regex for a https URL:
   regex = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
   ```
