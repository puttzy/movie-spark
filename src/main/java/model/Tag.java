package model;

import org.apache.spark.sql.Row;

public class Tag {
    private int userId;
    private String name;
    private int timestamp;

    private Tag(int userId, String name, int timestamp) {
        this.userId = userId;
        this.name = name;
        this.timestamp = timestamp;
    }

    public static Tag fromRow(Row row) {
        try {
            String name = row.getString(row.fieldIndex("tag"));
            int userId = row.getInt(row.fieldIndex("userId"));
            int timestamp =row.getInt(row.fieldIndex("timestamp"));
            return new Tag(userId, name, timestamp);
        } catch (IllegalArgumentException | UnsupportedOperationException exception) {
            exception.printStackTrace();
            return null;
        }
    }

    public int getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
