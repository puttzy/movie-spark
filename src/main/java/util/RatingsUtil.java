package util;

import model.Rating;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RatingsUtil {

    public static List<Double> extractRatingKeys(List<Rating> ratings) {
        List<Double> keys = new ArrayList<>();

        ratings.forEach(rating -> {
            if (rating.getScore() > 3.0) {
                keys.add(1.0);
            } else {
                keys.add(0.0);
            }
        });
        return keys;
    }

    public static List<Double> extractRatingScores(List<Rating> ratings) {
        return ratings.stream().map(Rating::getScore).collect(Collectors.toList());

    }
}
