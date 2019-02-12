package reader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.DatasetUtil;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CsvReader {

    private SparkSession spark;
    private Dataset<Row> moviesDataSet;
    private Dataset<Row> rankingsDataSet;
    private Dataset<Row> tagsDataSet;
    private Dataset<Row> ratedMoviesDataset;
    private Dataset<Row> taggedMoviesDataset;

    public CsvReader(String csvFilePath) {
        configureSparkSession();
        readDataSet(csvFilePath);
    }

    private void configureSparkSession() {
        SparkConf configuration = new SparkConf();
        configuration.setAppName("CSV Reader")
                .setMaster("local");

        spark = new SparkSession.Builder()
                .config(configuration)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    private void readDataSet(String filePath) {
        readMovieDataset(filePath);
        readRatingsDataset();
        readTagsDataset();
        joinDatasets();
    }

    private void readMovieDataset(String filePath) {
        moviesDataSet = spark
                .read()
                .format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filePath);
    }

    private void readRatingsDataset() {
        rankingsDataSet = spark
                .read()
                .format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/ratings.csv");
    }

    private void readTagsDataset() {
        tagsDataSet = spark
                .read()
                .format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/tags.csv");
    }

    private void joinDatasets() {
        ratedMoviesDataset = moviesDataSet.join(rankingsDataSet, rankingsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
        taggedMoviesDataset = moviesDataSet.join(tagsDataSet, tagsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
    }

    public Dataset<Row> getRowsWithTitle(String title) {
        return moviesDataSet.filter(moviesDataSet.col("title").contains(title));
    }

    public Dataset<Row> getRankingsForMovie(String title) {
        return ratedMoviesDataset.filter(ratedMoviesDataset.col("title").contains(title));
    }

    public Double getAverageRankingForMovie(String title) {
        Dataset<Row> firstKnightRankings = getRankingsForMovie(title);
        return firstKnightRankings.select(mean(firstKnightRankings.col("rating"))).collectAsList().get(0).getDouble(0);
    }

    public Double findTitleAndRatingCorrelation(String titleSubstring) {
        Dataset<Row> ratings = getRankingsForMovie(titleSubstring);
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaDoubleRDD xSeries = sparkContext.parallelizeDoubles(DatasetUtil.extractHashCodeValuesFromDataset(ratings, 1));
        JavaDoubleRDD ySeries = sparkContext.parallelizeDoubles(DatasetUtil.extractDoubleValuesFromDataset(ratings, 5));
        return Statistics.corr(xSeries.srdd(), ySeries.srdd(), "pearson");
    }

    public MultivariateStatisticalSummary summarizeRatingDataForTitle(String title) {
        Dataset<Row> ratings = getRankingsForMovie(title);
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Vector> distributedDataset = sparkContext.parallelize(DatasetUtil.transformDataSetIntoVectorList(ratings, 5));
        return Statistics.colStats(distributedDataset.rdd());
    }

    public List<String> findSimilarMovies(String title) {
        String tag = getTopTagForFilm(title);
        Dataset<Row> similarFilms = taggedMoviesDataset.filter(taggedMoviesDataset.col("tag").equalTo(tag));
        return DatasetUtil.extractStringsFromDataset(similarFilms, 1);
    }

    private String getTopTagForFilm(String title) {
        Dataset<Row> movies = moviesDataSet.filter(moviesDataSet.col("title").equalTo(title));
        Integer movieId = movies.collectAsList().get(0).getInt(0);
        Dataset<Row> tags = tagsDataSet.select(tagsDataSet.col("*"))
                .where(tagsDataSet.col("movieId").equalTo(movieId));
        RelationalGroupedDataset movieTags = tags.groupBy(tags.col("tag"));
        Dataset<Row> maxTag = movieTags.count().orderBy(col("count").desc());
        return maxTag.collectAsList().get(0).getString(0);
    }
}
