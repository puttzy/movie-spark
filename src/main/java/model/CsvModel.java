package model;

import org.apache.spark.sql.Row;

public interface CsvModel {
    <T> T fromRow(Row dataRow);
}