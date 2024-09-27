package com.navarchos.Navarchos;
import java.util.List;

/**
 * This class represents the structure of the response body received from the PdM API.
 * It is used to decode and store the data returned by the API.
 */
public class ResponseBody {
    private boolean alarm;
    private String description;
    private List<Integer> scores;
    private String source;
    private List<String> thresholds;
    private String timestamp;

    public ResponseBody() {
    }


    public boolean isAlarm() {
        return alarm;
    }

    public String getDescription() {
        return description;
    }

    public List<Integer> getScores() {
        return scores;
    }

    public String getSource() {
        return source;
    }

    public List<String> getThresholds() {
        return thresholds;
    }

    public String getTimestamp() {
        return timestamp;
    }
}