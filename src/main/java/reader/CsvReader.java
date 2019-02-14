package reader;

import model.Movie;
import model.Rating;
import model.Tag;
import org.apache.spark.sql.*;
import util.DatasetUtil;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CsvReader {

    private static final String MOVIE_ID = "movieId";
    private static final String RATING = "rating";
    private static final String TAG = "tag";
    private SparkSession sparkSession;
    private Dataset<Row> moviesDataSet;
    private Dataset<Row> rankingsDataSet;
    private Dataset<Row> tagsDataSet;
    private Dataset<Row> ratedMoviesDataset;
    private Dataset<Row> taggedMoviesDataset;


    private static final String FORMAT_CSV = "csv";
    private static final String INFER_SCHEMA = "inferSchema";
    private static final String HEADER = "header";
    private static final String SEPERATOR = ",";
    private static final String TRUE = "true";
    private static final String COMMA = ",";

    private static final String TITLE = "title";



    public CsvReader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        readDataSets();
    }

    private void readDataSets() {
        readMovieDataset();
        readRatingsDataset();
        readTagsDataset();
        joinDatasets();
    }

    private Dataset<Row> loadFile(String pathToFile){
        return sparkSession
                .read()
                .format(FORMAT_CSV)
                .option(SEPERATOR, COMMA)
                .option(INFER_SCHEMA, TRUE)
                .option(HEADER, TRUE)
                .load(pathToFile);
    }

    private void readMovieDataset() {
        moviesDataSet = loadFile("src/main/resources/movies.csv");
    }

    private void readRatingsDataset() {
        rankingsDataSet = loadFile("src/main/resources/ratings.csv");
    }

    private void readTagsDataset() {
        tagsDataSet = loadFile("src/main/resources/tags.csv");
    }

    private void joinDatasets() {
        ratedMoviesDataset = moviesDataSet.join(rankingsDataSet, rankingsDataSet.col(MOVIE_ID).equalTo(moviesDataSet.col(MOVIE_ID)));
        taggedMoviesDataset = moviesDataSet.join(tagsDataSet, tagsDataSet.col(MOVIE_ID).equalTo(moviesDataSet.col(MOVIE_ID)));
    }

    public List<Movie> getMoviesWithTitle(String title) {
        Dataset<Row> filteredMovies = filterDataset(moviesDataSet, TITLE, title);
        return deserializeMovies(filteredMovies);
    }

    public List<Rating> getRatingsForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, TITLE, title);
        return deserializeRatings(ratings);
    }

    public Double getAverageRankingForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, TITLE, title);
        return DatasetUtil.findAverageOfDataset(ratings, RATING);
    }

    public List<String> findSimilarMovies(String title) {
        String tag = getTopTagForFilm(title);
        Dataset<Row> similarFilms = taggedMoviesDataset.filter(taggedMoviesDataset.col(TAG).equalTo(tag));
        return DatasetUtil.extractStringsFromDataset(similarFilms, 1);
    }

    private Dataset<Row> filterDataset(Dataset<Row> dataset, String columnName, Object columnValue) {
        return dataset.filter(col(columnName).contains(columnValue));
    }

    private String getTopTagForFilm(String title) {
        Dataset<Row> movies = moviesDataSet.filter(moviesDataSet.col(TITLE).equalTo(title));
        Integer movieId = movies.collectAsList().get(0).getInt(0);
        Dataset<Row> tags = tagsDataSet.select(tagsDataSet.col("*"))
                .where(tagsDataSet.col(MOVIE_ID).equalTo(movieId));
        RelationalGroupedDataset movieTags = tags.groupBy(tags.col(TAG));
        Dataset<Row> maxTag = movieTags.count().orderBy(col("count").desc());
        return maxTag.collectAsList().get(0).getString(0);
    }

    private List<Movie> deserializeMovies(Dataset<Row> dataset) {
        List<Movie> movies = new ArrayList<>();
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
