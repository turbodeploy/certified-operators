package com.vmturbo.components.common.setting;

/**
 * Represents all possible counts of observation windows per day for timeslot feature.
 */
public enum DailyObservationWindowsCount {

    /**
     * 1 window per day.
     */
    ONE(1),

    /**
     * 2 window per day.
     */
    TWO(2),

    /**
     * 3 window per day.
     */
    THREE(3),

    /**
     * 4 window per day.
     */
    FOUR(4),

    /**
     * 6 window per day.
     */
    SIX(6);

    private final int countOfWindowsPerDay;

    DailyObservationWindowsCount(int countOfWindows) {
        this.countOfWindowsPerDay = countOfWindows;
    }

    @Override
    public String toString() {
        return String.format("%d windows per day", countOfWindowsPerDay);
    }

    public int getCountOfWindowsPerDay() {
        return countOfWindowsPerDay;
    }
}
