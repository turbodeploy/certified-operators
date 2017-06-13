package com.vmturbo.platform.analysis.utilities;

/**
 * A singleton class to manage StatsWriter instances.
 *
 * @author reshmakumar1
 *
 */
public class StatsManager {

    private StatsManager(){}
    private static StatsManager instance;
    private StatsWriter sw = null;

    static {
        instance = new StatsManager();
    }

    /**
     * Original design assumed there would be a few markets and multiple
     * threads writing to the stats file for a market.  But the use case
     * as of now is multiple topology-id's for the same market (or context),
     * therefore instead of multiple writers, we can choose to call StatsWriter
     * with a false argument, which will create just one writer that knows
     * which file to route the queued writes to.
     *
.    * @param filename
     * @return
     */
    public synchronized StatsWriter init(String filename) {
        if (sw == null || !sw.isAlive()) {
            sw = new StatsWriter(filename, false);
            sw.start();
        } else {
            sw.setFileName(filename);
        }
        return sw;
    }

    public static StatsManager getInstance() {
        return instance;
    }

}