package model;

import org.apache.spark.sql.Row;

public abstract class CsvModel {
    public abstract <T> T fromRow(Row dataRow);
}