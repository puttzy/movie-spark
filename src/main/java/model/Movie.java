package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.NonNull;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class Movie {
    @NonNull private int id;
    @NonNull private String title;
    @NonNull private List<String> genres;
    private List<Tag> tags;
    private List<Rating> ratings;


    public Movie(Row row) {
        this.id = row.getInt(row.fieldIndex("movieId"));
        this.title = row.getString(row.fieldIndex("title"));
        this.genres = Arrays.asList(row.getString(2).split("\\|"));

    }


    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

}