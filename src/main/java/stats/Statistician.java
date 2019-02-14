package stats;

import model.Rating;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.RatingsUtil;
import util.VectorUtil;

import java.util.Collections;
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

    public LogisticRegressionModel calculateBinomialRegression(Dataset<Row> dataset) {
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols((new String[]{"timestamp"}))
                .setOutputCol("features");

        Dataset<Row> assembledDataset = assembler.transform(dataset);

        LogisticRegression regression = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setLabelCol("timestamp")
                .setFeaturesCol("features");

        return regression.fit(assembledDataset);
    }
}
