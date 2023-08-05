This Java program connects to a SAP HANA database using the Apache Spark SQL DataFrames API, retrieves data from a specified table, and then writes this data to a Hive table.

Here's a detailed explanation of the code:

The host, port, username, and password for the SAP HANA database are defined.
The JDBC URL for the SAP HANA database is constructed using the specified host and port.
The schema and table name from which data will be retrieved from the SAP HANA database are defined.
The SQL query to retrieve data from the SAP HANA database is constructed.
A SparkSession is created with the application name "SparkConnector", configured to use all available cores for local execution and with Hive support enabled. The Hive warehouse directory is set to an HDFS location.
The spark.read() method is used to connect to the SAP HANA database using the provided JDBC URL, username, and password.
The SQL query is executed on the SAP HANA database and the result is loaded into a Spark DataFrame.
The DataFrame is written to a Hive table named "EXP.DEM_final_0122", overwriting it if it already exists.
The SparkSession is stopped.
Please note that this program requires the appropriate JDBC driver for SAP HANA to be available in the classpath. The specific driver used will depend on the version of the SAP HANA database.
