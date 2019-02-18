package reader;

import model.Movie;
import model.Rating;
import model.record.MovieRecord;
import org.apache.spark.sql.*;
import util.DatasetUtil;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

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
        moviesDataSet = readDataset("src/main/resources/movies.csv");
        rankingsDataSet = readDataset("src/main/resources/ratings.csv");
        tagsDataSet = readDataset("src/main/resources/tags.csv");

        joinDatasets();
    }

    private Dataset<Row> loadFile(String pathToFile) {
        return sparkSession
                .read()
                .format(FORMAT_CSV)
                .option(SEPERATOR, COMMA)
                .option(INFER_SCHEMA, TRUE)
                .option(HEADER, TRUE)
                .load(pathToFile);
    }

    private Dataset<Row> readDataset(String filePath) {
        return loadFile(filePath);
    }


    private void joinDatasets() {
        ratedMoviesDataset = moviesDataSet.join(rankingsDataSet, rankingsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
        taggedMoviesDataset = moviesDataSet.join(tagsDataSet, tagsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
    }


    public List<Movie> getMoviesWithTitle(String title) {
        Dataset<Row> filteredMovies = filterDataset(moviesDataSet, TITLE, title);
        return deserializeRow(filteredMovies, Movie::fromRow);
    }

    public List<Rating> getRatingsForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, TITLE, title);
        return deserializeRow(ratings, Rating::fromRow);
    }

    public Double getAverageRankingForMovie(String title) {
        Dataset<Row> ratings = filterDataset(ratedMoviesDataset, TITLE, title);
        return DatasetUtil.findAverageOfDataset(ratings, RATING);
    }

    public List<String> findSimilarMovies(String title) {
        String tag = getTopTagForFilm(title);
        System.out.println("Primary Tag for ".concat(title).concat(" is:").concat(tag));
        Dataset<Row> similarFilms = taggedMoviesDataset.filter(col(TAG).equalTo(tag)).select(col(TITLE)).distinct();
        return DatasetUtil.extractStringsFromDataset(similarFilms, 0);
    }


    public Dataset<Row> getRawRankingDatasetForMovie(String title) {
        return filterDataset(ratedMoviesDataset, TITLE, title);
    }

    private Dataset<Row> filterDataset(Dataset<Row> dataset, String columnName, Object columnValue) {
        return dataset.filter(col(columnName).contains(columnValue));
    }

    private Integer getMovieIdByTitle(String title) {
        return moviesDataSet.filter(moviesDataSet.col(TITLE).equalTo(title)).first().getInt(0);
    }

    private String getTopTagForFilm(String title) {
        return tagsDataSet
                .where(col(MOVIE_ID).equalTo(getMovieIdByTitle(title)))
                .select(col("tag"))
                .groupBy(col("tag"))
                .agg(count(col("tag")).alias("total"))
                .orderBy(col("total").desc()).first().getString(0);
    }


    private <T> List<T> deserializeRow(Dataset<Row> dataset, Function<Row, T> parseRowFunction) {
        return dataset.collectAsList()
                .stream()
                .map(parseRowFunction)
                .collect(Collectors.toList());

    }

    public void findMoviesUsingView(String title) {
        this.filterDataset(moviesDataSet, TITLE, title).createOrReplaceTempView("movies");
        sparkSession.sql("SELECT * FROM movies").show();
    }

    public Dataset<MovieRecord> findSerializedDataset() {
        Encoder movieEncoder = Encoders.bean(MovieRecord.class);
        return sparkSession
                .read()
                .format(FORMAT_CSV)
                .option(SEPERATOR, COMMA)
                .option(INFER_SCHEMA, TRUE)
                .option(HEADER, TRUE)
                .load("src/main/resources/movies.csv")
                .as(movieEncoder);
    }
}
