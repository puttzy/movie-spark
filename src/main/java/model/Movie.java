package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor(access= AccessLevel.PRIVATE)
@NoArgsConstructor
public class Movie {
    @NonNull private int id;
    @NonNull private String title;
    @NonNull private List<String> genres;
    private List<Tag> tags;
    private List<Rating> ratings;


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