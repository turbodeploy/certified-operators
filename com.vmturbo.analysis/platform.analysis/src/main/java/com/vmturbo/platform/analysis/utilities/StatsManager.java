package com.vmturbo.platform.analysis.utilities;

import java.util.HashMap;
import java.util.Map;

/**
 * A singleton class to manage StatsWriter instances.
 *
 * @author reshmakumar1
 *
 */
public class StatsManager {

    private StatsManager(){}
    private static StatsManager instance;
    // Map of writers -- one for each market that is running or has run
    private static final Map<String, StatsWriter> writersMap = new HashMap<>();

    static {
        instance = new StatsManager();
    }

    public StatsWriter init(String filename) {
        StatsWriter writerForThisFile = writersMap.get(filename);
        if((writerForThisFile == null) || !writerForThisFile.isAlive()) {
            StatsWriter sw = new StatsWriter(filename);
            sw.start();
            writersMap.put(filename, sw);
            return sw;
        }
        else {
            return writerForThisFile;
        }
    }

    public static StatsManager getInstance() {
        return instance;
    }

    public void remove(String filename) {

    }
}