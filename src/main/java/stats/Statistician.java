package stats;

import model.Rating;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;
import util.RatingsUtil;
import util.VectorUtil;

import java.util.List;

public class Statistician {
    private SparkSession sparkClient;

    private Statistician(SparkSession sparkClient) {
        this.sparkClient = sparkClient;
    }

    public static Statistician withSparkSession(SparkSession sparkClient) {
        return new Statistician(sparkClient);
    }

    public Double correlateRatingsToHighScore(List<Rating> ratings) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkClient.sparkContext());
        JavaDoubleRDD xSeries = sparkContext.parallelizeDoubles(RatingsUtil.extractRatingKeys(ratings));
        JavaDoubleRDD ySeries = sparkContext.parallelizeDoubles(RatingsUtil.extractRatingScores(ratings));
        return Statistics.corr(xSeries.srdd(), ySeries.srdd(), "pearson");
    }

    public MultivariateStatisticalSummary summarizeRatingStatistics(List<Rating> ratings) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkClient.sparkContext());
        JavaRDD<Vector> distributedDataset = sparkContext.parallelize(VectorUtil.createVectorsFromRatings(ratings));
        return Statistics.colStats(distributedDataset.rdd());
    }
}
