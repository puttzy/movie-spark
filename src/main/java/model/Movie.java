package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.*;
import model.record.MovieRecord;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Movie implements Serializable {
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

    public static Movie fromRecord(MovieRecord record) {
        return new Movie(record.getMovieId(), record.getTitle(), Arrays.asList(record.getGenres().split("\\|")));
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

}