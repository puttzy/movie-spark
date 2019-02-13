package model;

import org.apache.spark.sql.Row;

public class Rating {
    private int userId;
    private double score;
    private int timestamp;

    private Rating(int userId, double score, int timestamp) {
        this.userId = userId;
        this.score = score;
        this.timestamp = timestamp;
    }

    public static Rating fromRow(Row row) {
        int userId = row.getInt(row.fieldIndex("userId"));
        double score = row.getDouble(row.fieldIndex("rating"));
        int timestamp = row.getInt(row.fieldIndex("timestamp"));
        return new Rating(userId, score, timestamp);
    }

    public int getUserId() {
        return userId;
    }

    public double getScore() {
        return score;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
