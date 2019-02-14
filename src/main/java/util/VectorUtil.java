package util;

import model.Rating;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.List;
import java.util.stream.Collectors;

public class VectorUtil {

    public static List<Vector> createVectorsFromRatings(List<Rating> ratings) {
        return ratings
                .stream()
                .map(rating -> Vectors.dense(rating.getScore()))
                .collect(Collectors.toList());
    }
}
