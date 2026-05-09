package chicago.energy.spark;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class EnergyKafkaStream {

    private static final String KAFKA_BROKER = "kafka:9092";
    private static final String TOPIC = "energy_topic";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("ChicagoEnergy_Kafka_Structured")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        // 1. SCHÉMA ADAPTÉ DATASET
        StructType schema = new StructType()
                .add("year", DataTypes.StringType)
                .add("property_name", DataTypes.StringType)
                .add("community", DataTypes.StringType)
                .add("building_type", DataTypes.StringType)
                .add("eui", DataTypes.DoubleType)
                .add("ghg", DataTypes.DoubleType)
                .add("ghg_intensity", DataTypes.DoubleType)
                .add("energy_star", DataTypes.DoubleType)
                .add("electricity", DataTypes.DoubleType)
                .add("naturalgas", DataTypes.DoubleType);

        // 2. LECTURE KAFKA
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .load();
        // 3. PARSING JSON
        Dataset<Row> data = kafkaStream
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                .filter("eui IS NOT NULL AND ghg IS NOT NULL");
        // ANALYSE 1 : EUI PAR TYPE
        Dataset<Row> euiByType = data
                .groupBy("building_type")
                .agg(
                        avg("eui").alias("avg_eui"),
                        count("*").alias("count")
                )
                .orderBy(desc("avg_eui"));

        StreamingQuery q1 = euiByType.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("numRows", 20)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        // ANALYSE 2 : GHG PAR QUARTIER
        Dataset<Row> ghgByCommunity = data
                .groupBy("community")
                .agg(
                        sum("ghg").alias("total_ghg"),
                        avg("ghg").alias("avg_ghg")
                )
                .orderBy(desc("total_ghg"));
        StreamingQuery q2 = ghgByCommunity.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("numRows", 15)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        // ANALYSE 3 : ENERGY STAR
        Dataset<Row> starByType = data
                .groupBy("building_type")
                .agg(
                        avg("energy_star").alias("avg_energy_star")
                )
                .orderBy(desc("avg_energy_star"));
        StreamingQuery q3 = starByType.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("numRows", 15)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        System.out.println("Spark Streaming started... waiting for Kafka data");
        spark.streams().awaitAnyTermination();
    }
}