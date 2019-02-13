package util;

import model.Rating;

import java.util.ArrayList;
import java.util.List;

public class RatingsUtil {

    public static List<Double> extractRatingKeys(List<Rating> ratings) {
        List<Double> keys = new ArrayList<>();
        for (Rating rating : ratings) {
            if (rating.getScore() > 3.0) {
                keys.add(1.0);
            } else {
                keys.add(0.0);
            }
        }
        return keys;
    }

    public static List<Double> extractRatingScores(List<Rating> ratings) {
        List<Double> scores = new ArrayList<>();
        for (Rating rating : ratings) {
            scores.add(rating.getScore());
        }
        return scores;
    }
}
