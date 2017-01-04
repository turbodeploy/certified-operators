package com.vmturbo.platform.analysis.utilities;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class is for internal use to log stats to file.
 *
 * It logs to a file named by the date and market name, so that a market and plan running in parallel
 * do not conflict.  It has a .csv extension to allow it to open in excel.
 * TODO: Using FileLock.lock() if multiple threads write to a stats file with the same name at the same time.
 * Replace with another locking mechanism if FileLock is not the most appropriate
 *
 * @author reshmakumar1
 *
 */

public class StatsUtils {

    static final Logger logger = Logger.getLogger(StatsUtils.class);
    // Delimiters used in CSV file
    private static final String fieldSeparator = ",";
    private static final String recordSeparator = "\n";
    private static final String statsFilePrefix = Paths.get(".").toAbsolutePath().normalize().toString() + File.separator
                     + "data" + File.separator + "repos" + File.separator;

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat simpleDateFormatWithTime =
                                                                   new SimpleDateFormat("MM-DD-YY-HH:SS");
    private static final SimpleDateFormat simpleTimeFormat =
                                                           new SimpleDateFormat("h:mm a");
    private String statsFileName;
    private StringBuilder dataBuffer = new StringBuilder();
    private Instant before;
    private boolean enabled = true;

    public StatsUtils(@NonNull String filename, boolean isEnabled) {
        if(!isEnabled()) {
            return;
        }
        statsFileName = statsFilePrefix + filename + ".csv";
        setEnabled(isEnabled);
    }

    /**
     * Returns the stats are enabled.
     *
     * Stats are written to file daily if enabled is true.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the <b>enableStats<b> flag.
     *
     */
    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Write the internal buffer to file.
     *
     * @param isLast is the data going to be at the end of a line.
     */
    public void flush(boolean isLast) {
        if (!isEnabled()) {
            return;
        }
        try (FileWriter fileWriter = new FileWriter(statsFileName, true)) {
            // append data to file
            String data = dataBuffer.toString();
            if (isLast) {
                fileWriter.append(data + recordSeparator);
            } else {
                if (data.length() > 0) {
                    fileWriter.append(data);
                    if (!data.substring(data.length() - 1).equals(fieldSeparator)) {
                        fileWriter.append(fieldSeparator);
                    }
                }
            }
            // clear internal buffer after writing
            dataBuffer.setLength(0);
        }
        catch (Exception e) {
            logger.error("Error in Csv File Writer: ", e);
        }
    }

    /**
    * Write data to file, an object.
    * Note that append also writes to file, and appendAtEnd writes to end of a line.
    * This method allows user to specify if data will be written to end of line or not.
    * And by default it's also the first bit of data to be written to the file.
    *
    * @param data to write.
    * @param isLast is the data going to be at the end of a line.
    */
    public void write(@NonNull Object data, boolean isLast) {
        if (!isEnabled()) {
            return;
        }
        if (data != null) {
            dataBuffer.setLength(0);
            dataBuffer.append(data);
        }
        flush(isLast);
    }

    /**
    * Write the internal buffer to file.
    * By default, its not the last bit of data written to the line.
    */
    private void flush() {
        if(!isEnabled()) {
            return;
        }
        flush(false);
    }

    /**
     * Write to first line of the file only.
     *
     * @param header text to be written to the first line of the file such as column names.
     */
    public void appendHeader(@NonNull String header) {
        if(!isEnabled()) {
            return;
        }
        File file = new File(statsFileName);
        if (file.length() == 0L) {
            write(header, true);
        }
    }

    private static String getDate() {
        return simpleDateFormat.format(new Date());
    }

    private static String getTime() {
        return simpleTimeFormat.format(new Date());
    }

    private static String getDateTime() {
        return simpleDateFormatWithTime.format(new Date());
    }

    /*
     * Get duration in milliseconds.
     *
     */
    public static long getDurationInMilliSec(@NonNull Instant earlier, @NonNull Instant later) {
        return Duration.between(earlier, later).toMillis();
    }

    public static Instant getInstant() {
        return Instant.now();
    }

    /**
     * Instant now.
     *
     */
    public Instant before() {
        before = Instant.now();
        return before;
    }

    /**
     * Duration between last call to before() and now.
     *
     * @param isFirst is this the first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void after(boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(getDurationInMilliSec(before, Instant.now()), isFirst, doFlush);
    }

    /**
     * Duration between last call to before() and now.
     * No arguments implies arguments of false meaning it's not the first.
     * in a list of values being written and it's not to be written to file yet.
     */
    public void after() {
        if(!isEnabled()) {
            return;
        }
        after(false, false);
    }

    /**
     * Duration between Instant prior and now.
     *
     * @param prior.
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void after(@NonNull Instant prior, boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(getDurationInMilliSec(prior, Instant.now()), isFirst, doFlush);
    }

    /**
     * Duration between Instant prior and now.
     * By default it's not the first in a list of values being written and
     * it's not to be written to file yet.
     *
     * @param prior
     */
    public void after(@NonNull Instant prior) {
        if(!isEnabled()) {
            return;
        }
        append(getDurationInMilliSec(prior, Instant.now()));
    }

    /**
     * Build a comma separated list of values to add to file.  Write if doFlush is true.
     *
     * @param data data to write.
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void append(@NonNull Object data, boolean isFirst, boolean doFlush) {
        if (!isEnabled()) {
            return;
        }
        if (isFirst) {
            dataBuffer.setLength(0);
        }
        dataBuffer.append(data + fieldSeparator);
        if (doFlush) {
            flush();
        }
    }

    /**
     * By default data is not the first in a list of values being written and
     * it's not to be written to file yet.
     *
     * @param data data to write.
     */
    public void append(@NonNull Object data) {
        if(!isEnabled()) {
            return;
        }
        append(data, false, false);
    }

    /**
     * Build a comma separated list of values to add to file, at the end of a row.  Write if doFlush is true.
     *
     * @param data data to write.
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendAtEnd(@NonNull Object data, boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(data, false, true);
        flush(true);
    }

    /**
     * concatenate date and time to be logged.
     *
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendDateTime(boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(getDateTime(), false, doFlush);
    }

    /**
     * concatenate time to be logged.
     *
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendTime(boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(getTime(), false, doFlush);
    }

    /**
     * concatenate date to be logged.
     *
     * @param isFirst is this first value in a list of values being written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendDate(boolean isFirst, boolean doFlush) {
        if(!isEnabled()) {
            return;
        }
        append(getDate(), false, doFlush);
    }
}
