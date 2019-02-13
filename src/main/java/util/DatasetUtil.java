package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;

public class DatasetUtil {

    public static Double findAverageOfDataset(Dataset<Row> dataset, String columnName) {
        return dataset.select(mean(col(columnName))).collectAsList().get(0).getDouble(0);
    }

    public static List<String> extractStringsFromDataset(Dataset<Row> dataset, int columnIndex) {
        List<String> strings = new ArrayList<String>();
        for (Row row : dataset.collectAsList()) {
            strings.add(row.getString(columnIndex));
        }
        return strings;
    }
}
