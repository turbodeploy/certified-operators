package com.vmturbo.history.db;

import java.util.Calendar;

public enum TimeFrame {
    LATEST(0, "latest", Calendar.MINUTE),
    HOUR(1, "hour", Calendar.HOUR_OF_DAY),
    DAY(2, "day", Calendar.DAY_OF_MONTH),
    MONTH(3, "month", Calendar.MONTH),
    YEAR(4, "year", Calendar.YEAR);

    private final int value;
    private final String suffix;
    private final int timeInterval;

    TimeFrame(int val, String tFrameSuff, int time) {
        suffix = tFrameSuff;
        value = val;
        timeInterval = time;
    }

    public int getValue() {
        return value;
    }

    public String getSuffix() {
        return suffix;
    }

    /**
     * Returns this TimeFram's time interval.
     */
    public int getTimeInterval() {
        return timeInterval;
    }

    public TimeFrame getPrev() {
        switch (this) {
            case MONTH:
                return DAY;
            case DAY:
                return HOUR;
            default:
                return null;
        }
    }

    public TimeFrame getNext() {
        switch (this) {
            case HOUR:
                return DAY;
            case DAY:
                return MONTH;
            default:
                return null;
        }
    }

    /**
     * MUST be ordered to account for dependencies between tables
     */
    public static final TimeFrame[] ROLLED_UP_TIME_FRAMES = {TimeFrame.HOUR, TimeFrame.DAY};
}