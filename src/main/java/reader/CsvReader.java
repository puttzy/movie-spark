package reader;

import model.Movie;
import model.Rating;
import model.Tag;
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

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CsvReader {

    private SparkSession spark;
    private Dataset<Row> moviesDataSet;
    private Dataset<Row> rankingsDataSet;
    private Dataset<Row> tagsDataSet;
    private Dataset<Row> ratedMoviesDataset;
    private Dataset<Row> taggedMoviesDataset;

    public CsvReader(SparkSession sparkSession) {
        this.spark = sparkSession;
        readDataSet();
    }

    private void readDataSet() {
        readMovieDataset();
        readRatingsDataset();
        readTagsDataset();
        joinDatasets();
    }

    private void readMovieDataset() {
        moviesDataSet = spark
                .read()
                .format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/movies.csv");
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

    public List<Movie> getMoviesWithTitle(String title) {
        Dataset<Row> filteredMovies = filterDataset(moviesDataSet, "title", title);
        return deserializeMovies(filteredMovies);
    }

    public List<Rating> getRatingsForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, "title", title);
        return deserializeRatings(ratings);
    }

    public Double getAverageRankingForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, "title", title);
        return DatasetUtil.findAverageOfDataset(ratings, "rating");
    }

    public List<String> findSimilarMovies(String title) {
        String tag = getTopTagForFilm(title);
        Dataset<Row> similarFilms = taggedMoviesDataset.filter(taggedMoviesDataset.col("tag").equalTo(tag));
        return DatasetUtil.extractStringsFromDataset(similarFilms, 1);
    }

    private Dataset<Row> filterDataset(Dataset<Row> dataset, String columnName, Object columnValue) {
        return dataset.filter(col(columnName).contains(columnValue));
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

    private List<Movie> deserializeMovies(Dataset<Row> dataset) {
        List<Movie> movies = new ArrayList<Movie>();
        for (Row row : dataset.collectAsList()) {
            movies.add(Movie.fromRow(row));
        }
        return movies;
    }

    private List<Tag> deserializeTags(Dataset<Row> dataset) {
        List<Tag> tags = new ArrayList<>();
        for (Row row : dataset.collectAsList()) {
            tags.add(Tag.fromRow(row));
        }
        return tags;
    }

    private List<Rating> deserializeRatings(Dataset<Row> dataset) {
        List<Rating> ratings = new ArrayList<>();
        for (Row row : dataset.collectAsList()) {
            ratings.add(Rating.fromRow(row));
        }
        return ratings;
    }
}
