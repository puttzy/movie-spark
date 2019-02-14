package reader;

import model.CsvModel;
import model.Movie;
import model.Rating;
import model.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.DatasetUtil;

import java.util.List;
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
        ratedMoviesDataset = moviesDataSet.join(rankingsDataSet, rankingsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
        taggedMoviesDataset = moviesDataSet.join(tagsDataSet, tagsDataSet.col("movieId").equalTo(moviesDataSet.col("movieId")));
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
        String tag = fastGetTopTagForFilm(title);
        tag = getTopTagForFilm(title);

        Dataset<Row> similarFilms = taggedMoviesDataset.filter(taggedMoviesDataset.col(TAG).equalTo(tag)).distinct();
        return DatasetUtil.extractStringsFromDataset(similarFilms, 1);
    }

    private Dataset<Row> filterDataset(Dataset<Row> dataset, String columnName, Object columnValue) {
        return dataset.filter(col(columnName).contains(columnValue));
    }

    private String getTopTagForFilm(String title) {
        long startTime = System.currentTimeMillis();
        Dataset<Row> movies = moviesDataSet.filter(moviesDataSet.col(TITLE).equalTo(title));
        Integer movieId = movies.collectAsList().get(0).getInt(0);
        Dataset<Row> tags = tagsDataSet.select(tagsDataSet.col("*"))
                .where(tagsDataSet.col(MOVIE_ID).equalTo(movieId));
        RelationalGroupedDataset movieTags = tags.groupBy(tags.col(TAG));
        Dataset<Row> maxTag = movieTags.count().orderBy(col("count").desc());
        String s = maxTag.collectAsList().get(0).getString(0);
        System.out.println("(old) returning tag. (" + s + ") in " + (System.currentTimeMillis() - startTime) + "ms");
        return s;
    }

    private Integer getMovieIdByTitle(String title) {
        return moviesDataSet.filter(moviesDataSet.col(TITLE).equalTo(title)).first().getInt(0);
    }

    private String fastGetTopTagForFilm(String title) {
        long startTime = System.currentTimeMillis();

        String s = tagsDataSet
                .where(col(MOVIE_ID).equalTo(getMovieIdByTitle(title)))

                .select(col("tag"))
                .groupBy(col("tag"))
                .agg(count(col("tag")).alias("total"))
                .orderBy(col("total").desc()).first().getString(0);


        System.out.println("(new) returning tag. (" + s + ") in " + (System.currentTimeMillis() - startTime) + "ms");
        return s;
    }


    private List<CsvModel> deserializeRow(Dataset<Row> dataset, Class<CsvModel> clazz) throws IllegalAccessException, InstantiationException {
        CsvModel csvModel = clazz.newInstance();
/*        return dataset.collectAsList()
                .stream()
                .map(r -> csvModel.fromRow(r))
                .collect(Collectors.toList());*/

        return null;
    }


    private List<Movie> deserializeMovies(Dataset<Row> dataset) {
        return dataset.collectAsList()
                .stream()
                .map(Movie::fromRow)
                .collect(Collectors.toList());
    }

    private List<Tag> deserializeTags(Dataset<Row> dataset) {
        return dataset.collectAsList()
                .stream()
                .map(Tag::fromRow)
                .collect(Collectors.toList());
    }

    private List<Rating> deserializeRatings(Dataset<Row> dataset) {
        return dataset.collectAsList()
                .stream()
                .map(Rating::fromRow)
                .collect(Collectors.toList());
    }
}
