package chicago.energy.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class EnergyAnalysis {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: EnergyAnalysis <input> <output>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("ChicagoEnergyAnalysis")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // =========================
        // LOAD DATASET
        // =========================
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .option("escape", "\"")
                .csv(args[0]);

        // =========================
        // CLEANING + CASTING
        // =========================
        df = df
                .withColumn("SiteEUI", col("`Site EUI (kBtu/sq ft)`").cast("double"))
                .withColumn("GHG", col("`Total GHG Emissions (Metric Tons CO2e)`").cast("double"))
                .withColumn("EnergyStar", col("`ENERGY STAR Score`").cast("double"))
                .withColumn("Year", col("Data Year").cast("int"))
                .withColumn("CommunityNorm", lower(trim(col("Community Area"))))
                .withColumn("Electricity", col("`Electricity Use (kBtu)`").cast("double"))
                .withColumn("Gas", col("`Natural Gas Use (kBtu)`").cast("double"));

        df.cache();

        System.out.println("TOTAL ROWS = " + df.count());

        // =========================
        // ANALYSE 1
        // =========================
        Dataset<Row> eui = df
                .filter(col("SiteEUI").isNotNull())
                .groupBy("Primary Property Type")
                .agg(avg("SiteEUI").alias("avg_eui"),
                        count("*").alias("count"))
                .orderBy(desc("avg_eui"));

        eui.show(false);
        eui.write().mode("overwrite")
                .csv(args[1] + "/analysis1");

        // =========================
        // ANALYSE 2
        // =========================
        Dataset<Row> ghg = df
                .filter(col("GHG").isNotNull())
                .groupBy("CommunityNorm")
                .agg(sum("GHG").alias("total_ghg"))
                .orderBy(desc("total_ghg"));

        ghg.show(false);
        ghg.write().mode("overwrite")
                .csv(args[1] + "/analysis2");

        // =========================
        // ANALYSE 3
        // =========================
        Dataset<Row> star = df
                .filter(col("EnergyStar").isNotNull())
                .groupBy("Year")
                .agg(avg("EnergyStar").alias("avg_score"))
                .orderBy("Year");

        star.show(false);
        star.write().mode("overwrite")
                .csv(args[1] + "/analysis3");

        // =========================
        // ANALYSE 4
        // =========================
        Dataset<Row> top = df
                .filter(col("GHG").isNotNull())
                .orderBy(desc("GHG"))
                .limit(20);

        top.show(false);
        top.write().mode("overwrite")
                .csv(args[1] + "/analysis4");

        // =========================
        // ANALYSE 5
        // =========================
        Dataset<Row> mix = df
                .groupBy("Primary Property Type")
                .agg(
                        avg("Electricity").alias("avg_elec"),
                        avg("Gas").alias("avg_gas")
                );

        mix.show(false);
        mix.write().mode("overwrite")
                .csv(args[1] + "/analysis5");

        spark.stop();
    }
}