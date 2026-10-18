package optimization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor

object YieldOptimizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AirbnbYieldOptimizer")
      .getOrCreate()

    import spark.implicits._

    val hdfsBase = "hdfs://127.0.0.1:9000"
    val baseData = spark.read.parquet(s"$hdfsBase/airbnb/processed/segmented_listings")

    val neighborhoodMedians = baseData
      .groupBy("neighbourhood")
      .agg(expr("percentile_approx(listed_price, 0.5)").alias("neighbourhood_median_price"))

    val enriched = baseData
      .join(neighborhoodMedians, Seq("neighbourhood"), "left")
      .withColumn(
        "price_ratio_to_median",
        when($"neighbourhood_median_price" > 0,
          $"listed_price" / $"neighbourhood_median_price").otherwise(1.0)
      )

    val roomTypeIndexer = new StringIndexer()
      .setInputCol("room_type")
      .setOutputCol("room_type_idx")
      .setHandleInvalid("keep")

    val indexed = roomTypeIndexer.fit(enriched).transform(enriched)

    val featureCols = Array(
      "accommodates", "bedrooms", "beds", "listed_price",
      "price_ratio_to_median", "minimum_nights", "number_of_reviews",
      "review_scores_rating", "reviews_per_month", "market_regime",
      "room_type_idx"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val modelingData = assembler.transform(indexed)
      .withColumn("label", col("forward_occupancy_rate"))

    val gbt = new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxDepth(5)
      .setMaxIter(30)
      .setSeed(42L)

    val gbtModel = gbt.fit(modelingData)
    val multipliers = Seq(0.70, 0.85, 1.00, 1.15, 1.30)

    val simulatedFrames = multipliers.map { multiplier =>
      val simData = indexed
        .withColumn("sim_price", round($"listed_price" * multiplier, 2))
        .withColumn(
          "price_ratio_to_median",
          when($"neighbourhood_median_price" > 0,
            ($"listed_price" * multiplier) / $"neighbourhood_median_price").otherwise(1.0)
        )
        .withColumn("original_price", $"listed_price")
        .withColumn("listed_price", $"sim_price")

      val simFeatures = assembler.transform(simData)

      gbtModel.transform(simFeatures)
        .withColumn(
          "pred_occupancy",
          when($"prediction" > 1.0, 1.0)
            .when($"prediction" < 0.0, 0.0)
            .otherwise($"prediction")
        )
        .withColumn("sim_revpar", round($"sim_price" * $"pred_occupancy", 2))
        .select(
          $"listing_id", $"original_price", $"current_revpar", $"sim_price",
          $"pred_occupancy", $"sim_revpar"
        )
    }

    val consolidatedSimulations = simulatedFrames.reduce(_.unionByName(_))
    val listingWindow = Window.partitionBy("listing_id").orderBy(col("sim_revpar").desc)

    val optimalDecisions = consolidatedSimulations
      .withColumn("rank", row_number().over(listingWindow))
      .filter($"rank" === 1)
      .select(
        $"listing_id",
        $"original_price".alias("current_price"),
        $"sim_price".alias("optimal_price"),
        $"current_revpar",
        $"sim_revpar".alias("optimal_revpar"),
        round(
          ($"sim_revpar" - $"current_revpar") /
            when($"current_revpar" < 5.0, 5.0).otherwise($"current_revpar") * 100,
          1
        ).alias("projected_uplift_pct")
      )
      .withColumn(
        "strategic_action",
        when($"optimal_price" >= $"current_price" * 1.15, "UNDERPRICED_YIELD_OPPORTUNITY")
          .when($"optimal_price" <= $"current_price" * 0.85, "OVERPRICED_VACANCY_TRAP")
          .otherwise("PRICE_OPTIMIZED_EQUILIBRIUM")
      )

    optimalDecisions.write
      .mode("overwrite")
      .parquet(s"$hdfsBase/airbnb/processed/yield_optimization_results")

    spark.stop()
  }
}