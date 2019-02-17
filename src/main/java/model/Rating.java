package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.Row;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Rating {
    private int userId;
    private double score;
    private int timestamp;

    public static Rating fromRow(Row row) {
        int userId = row.getInt(row.fieldIndex("userId"));
        double score = row.getDouble(row.fieldIndex("rating"));
        int timestamp = row.getInt(row.fieldIndex("timestamp"));
        return new Rating(userId, score, timestamp);
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}
