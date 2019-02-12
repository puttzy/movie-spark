package util;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class DatasetUtil {
    public static List<Double> extractDoubleValuesFromDataset(Dataset<Row> dataset, int columnIndex) {
        List<Double> values = new ArrayList<Double>();
        for (Row row : dataset.collectAsList()) {
            values.add(row.getDouble(columnIndex));
        }
        return values;
    }

    public static List<Double> extractHashCodeValuesFromDataset(Dataset<Row> dataset, int columnIndex) {
        List<Double> values = new ArrayList<Double>();
        for (Row row : dataset.collectAsList()) {
            Double value = (double) row.getString(columnIndex).hashCode();
            values.add(value);
        }
        return values;
    }

    public static List<Vector> transformDataSetIntoVectorList(Dataset<Row> dataset, int columnIndex) {
        List<Vector> vectors = new ArrayList<Vector>();
        for (Row row : dataset.collectAsList()) {
            Double value = row.getDouble(columnIndex);
            vectors.add(Vectors.dense(value));
        }
        return vectors;
    }

    public static List<String> extractStringsFromDataset(Dataset<Row> dataset, int columnIndex) {
        List<String> strings = new ArrayList<String>();
        for (Row row : dataset.collectAsList()) {
            strings.add(row.getString(columnIndex));
        }
        return strings;
    }
}
