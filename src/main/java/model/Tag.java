package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import org.apache.spark.sql.Row;

@Getter
public class Tag {

    private int userId;
    private String name;
    private int timestamp;

    public Tag(Row row) {
        this.name = row.getString(row.fieldIndex("tag"));
        this.userId = row.getInt(row.fieldIndex("userId"));
        this.timestamp = row.getInt(row.fieldIndex("timestamp"));

    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}
