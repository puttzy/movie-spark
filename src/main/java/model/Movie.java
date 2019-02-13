package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class Movie {
    private int id;
    private String title;
    private List<String> genres;
    private List<Tag> tags;
    private List<Rating> ratings;

    private Movie(int id, String title, List<String> genres) {
        this.id = id;
        this.title = title;
        this.genres = genres;
    }

    public static Movie fromRow(Row row) {
        int id = row.getInt(row.fieldIndex("movieId"));
        String title = row.getString(row.fieldIndex("title"));
        List<String> genres = Arrays.asList(row.getString(2).split("\\|"));
        return new Movie(id, title, genres);
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}