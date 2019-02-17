package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.Row;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Tag {

    private int userId;
    private String name;
    private int timestamp;

    public static Tag fromRow(Row row) {
        try {
            String name = row.getString(row.fieldIndex("tag"));
            int userId = row.getInt(row.fieldIndex("userId"));
            int timestamp = row.getInt(row.fieldIndex("timestamp"));
            return new Tag(userId, name, timestamp);
        } catch (IllegalArgumentException | UnsupportedOperationException exception) {
            exception.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}
