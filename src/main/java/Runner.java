import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import reader.CsvReader;

import java.util.List;
import java.util.Scanner;

public class Runner {

    private static boolean isFinished = false;
    private static CsvReader reader = new CsvReader("src/main/resources/movies.csv");
    private static Scanner keyboard;

    private static final int FIND_MOVIES = 1;
    private static final int FIND_NUMBER_OF_RATINGS = 2;
    private static final int FIND_AVERAGE_RATING = 3;
    private static final int FIND_CORRELATION = 4;
    private static final int SUMMARIZE_STATISTICS = 5;
    private static final int FIND_SIMILAR_MOVIES = 6;
    private static final int EXIT = 7;

    public static void main(String[] args) {
        configureInput();
        while (!isFinished) {
            printMenu();
            printSpace();
            readInput();
            printSpace();
        }
    }

    private static void configureInput() {
        keyboard = new Scanner(System.in);
    }

    private static void readInput() {
        int choice = keyboard.nextInt();
        keyboard.nextLine();
        switch (choice) {
            case FIND_MOVIES:
                findMoviesWithSubstring();
                break;
            case FIND_NUMBER_OF_RATINGS:
                findRankingsForMovie();
                break;
            case FIND_AVERAGE_RATING:
                findAverageRatingForMovie();
                break;
            case FIND_CORRELATION:
                findCorrelation();
                break;
            case SUMMARIZE_STATISTICS:
                summarizeDataForMovie();
                break;
            case FIND_SIMILAR_MOVIES:
                findSimilarMovies();
                break;
            case EXIT:
                isFinished = true;
                System.exit(0);
                break;
            default:
                System.out.println("Invalid option selected!");
        }
    }

    private static void printMenu() {
        System.out.println("Select an option: ");
        System.out.println("---------------------------------------------------------");
        System.out.println("1. Find movies with a certain string in the title.");
        System.out.println("2. Find the number of ratings for a specific title.");
        System.out.println("3. Find the AVG ratings for a specific title.");
        System.out.println("4. Find the correlation between a title and ratings.");
        System.out.println("5. Summarize statistical data for a specific movie title.");
        System.out.println("6. Find similar movies to a film.");
        System.out.println("7. Exit.");
    }

    private static void findMoviesWithSubstring() {
        String substring = keyboard.nextLine();
        Dataset<Row> movies = reader.getRowsWithTitle(substring);
        System.out.println("Movies with the title 'Knight': ".concat(String.valueOf(movies.count())));
        movies.show();
    }

    private static void findRankingsForMovie() {
        promptForTitle();
        String title = keyboard.nextLine();
        Dataset<Row> firstKnightRankings = reader.getRankingsForMovie(title);
        System.out.println("Number of ratings for the film ".concat(title).concat(": ".concat(String.valueOf(firstKnightRankings.count()))));
        firstKnightRankings.show();
    }

    private static void findAverageRatingForMovie() {
        promptForTitle();
        String title = keyboard.nextLine();
        System.out.println(reader.getAverageRankingForMovie(title));
    }

    private static void findCorrelation() {
        promptForTitle();
        String title = keyboard.nextLine();
        System.out.println("r = ".concat(String.valueOf(reader.findTitleAndRatingCorrelation(title))));
    }

    private static void summarizeDataForMovie() {
        promptForTitle();
        String title = keyboard.nextLine();
        MultivariateStatisticalSummary summary = reader.summarizeRatingDataForTitle(title);
        System.out.println("\u03C3₂ = ".concat(String.valueOf(summary.variance().toArray()[0])));
        System.out.println("max(rating) = ".concat(String.valueOf(summary.max().toArray()[0])));
        System.out.println("min(rating) = ".concat(String.valueOf(summary.min().toArray()[0])));
        System.out.println("avg(rating) = ".concat(String.valueOf(summary.mean().toArray()[0])));
    }

    private static void findSimilarMovies() {
        promptForTitle();
        String title = keyboard.nextLine();
        List<String> similarMovies = reader.findSimilarMovies(title);
        System.out.println("Films similar to '".concat(title).concat("': "));
        for (String movie : similarMovies) {
            System.out.println(movie);
        }
    }

    private static void printSpace() {
        System.out.println("\n");
    }

    private static void promptForTitle() {
        System.out.print("Enter a movie title substring: ");
    }
}
