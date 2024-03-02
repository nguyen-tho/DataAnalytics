A data analytics lecture using pyspark
1. Lession 1: Read, Write and validate data for csv file and json file
   
   - ex1.ipynb for csv data
   - ex2.ipynb for json data
   - dataset for homework: https://drive.google.com/file/d/1bw7pEgXSVLyMuaI_s3FPa5smNKHsu7-c/view?fbclid=IwAR1XUrTk0Oj0k26f2mS889ZkQEGx3FCI4i7rdO3zNVi5ZM-DpahqUCX8aN4
   - homework: exercises 1, 2, 3, 4, 5, 10 at Read_Write_and_Validate_HW.ipynb

2. Lesson 2: Data manipulation
   
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
   ```
   - dataset for this lesson: https://www.kaggle.com/datasets/datasnaek/youtube-new?fbclid=IwAR1GafFaK6Pm1-voK-LRwJGG8Lgk1QWEd09UE661dVNZcAHqfnR_5-4ybN8#USvideos.csv
   - ex2-1.ipynb for lession 1 revision
   - ex2-2.ipynb for Youtube trending dataset from above URL 


4. Lesson 3: Data manipulation (cont)
