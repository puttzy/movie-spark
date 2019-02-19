package model.record;

import lombok.Data;

import java.io.Serializable;

@Data
public class MovieRecord implements Serializable {
    private int movieId;
    private String title;
    private String genres;

}
