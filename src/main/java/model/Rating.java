package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import org.apache.spark.sql.Row;


@Getter
public class Rating {
    private int userId;
    private double score;
    private int timestamp;

    public Rating(Row row) {
        this.userId = row.getInt(row.fieldIndex("userId"));
        this.score = row.getDouble(row.fieldIndex("rating"));
        this.timestamp = row.getInt(row.fieldIndex("timestamp"));

    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}
