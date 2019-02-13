package util;

import model.Rating;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.List;

public class VectorUtil {

    public static List<Vector> createVectorsFromRatings(List<Rating> ratings) {
        List<Vector> vectors = new ArrayList<>();
        for (Rating rating : ratings) {
            vectors.add(Vectors.dense(rating.getScore()));
        }
        return vectors;
    }
}
