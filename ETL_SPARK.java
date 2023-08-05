
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class spark {
    public static void main(String[] args) {
        String host = "192.168.177.215";
        String port = "32315";
        String username = "BIG_DATA_USER";
        String password = "PASSWD))*123";
        String url = "jdbc:sap://" + host + ":" + port;

        String schema = "SSK";
        String tableName = "EXP/EMP/ST";
        String query = "(SELECT * FROM \"" + schema + "\".\"" + tableName + "\") as tmp";

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkConnector")
                .master("local[*]")  // Use all available cores for local execution
                .enableHiveSupport() // Enable Hive support
                .config("spark.sql.warehouse.dir", "hdfs://192.168.234.6:8020/warehouse/tablespace/external/hive") // Set Hive warehouse directory
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("user", username)
                .option("password", password)
                .option("dbtable", query)
                .load();

        // Specify the Hadoop Distributed File System (HDFS) output path
        // String hdfsOutputPath = "hdfs://192.168.247.6:8020/test1/NES-TES1-final";

        // df.write().option("header", "true").csv(hdfsOutputPath);

        // Write the dataframe to Hive table
        df.write().mode(SaveMode.Overwrite).format("hive").saveAsTable("nesto.nesto_final_0122"); // Use SaveMode.Overwrite if you want to overwrite existing data

        spark.stop();
    }
}

