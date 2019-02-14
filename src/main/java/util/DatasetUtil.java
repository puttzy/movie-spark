package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;

public class DatasetUtil {

    public static Double findAverageOfDataset(Dataset<Row> dataset, String columnName) {
        return dataset.select(mean(col(columnName))).collectAsList().get(0).getDouble(0);
    }

    public static List<String> extractStringsFromDataset(Dataset<Row> dataset, int columnIndex) {
        return dataset.collectAsList()
                .stream()
                .map(row -> row.getString(columnIndex))
                .collect(Collectors.toList());
    }
}
