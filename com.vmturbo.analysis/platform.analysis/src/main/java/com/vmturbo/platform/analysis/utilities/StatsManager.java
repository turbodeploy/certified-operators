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
     * as of now is different contextid/topologyid combinations
     * Real time context id is at present constant (777777)
     * Each plan will have a different context id, and topology id
     * therefore instead of multiple writers, we have a single StatsWriter
     * that knows where to route queued writes to.
     *
.    * @param filename
     */
    public synchronized StatsWriter init() {
        if (sw == null || !sw.isAlive()) {
            sw = new StatsWriter();
            sw.start();
        }
        return sw;
    }

    public static StatsManager getInstance() {
        return instance;
    }

}