package com.vmturbo.platform.analysis.utilities;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.apache.log4j.Logger;

public class StatsUtils {

    static final Logger logger = Logger.getLogger(StatsUtils.class);
    // Delimiters used in CSV file
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    // m2 stats file prefix
    private static final String M2_STATS_PATH_PREFIX = System.getProperty("user.dir") + File.separator
                                              + "data" + File.separator + "repos" + File.separator;
    // m2 stats file date and time format
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat simpleDateFormatWithTime =
                                                                   new SimpleDateFormat("MM-DD-YY-HH:SS");
    private static final SimpleDateFormat simpleTimeFormat =
                                                           new SimpleDateFormat("h:mm a");
    private String statsFileName = "DefaultStatsFile";
    private StringBuilder dataBuffer = new StringBuilder();
    private Instant before;

    public StatsUtils(String filename) {
        statsFileName = M2_STATS_PATH_PREFIX + filename + "-" + getDate() + ".csv";
    }

    /**
     * write the internal buffer to file.
     * @param isLast is the data going to be at the end of a line.
     */
    public void write(boolean isLast) {
        try (FileWriter fileWriter = new FileWriter(statsFileName, true)) {
            // append data to file
            String data = dataBuffer.toString();
            if (isLast) {
                fileWriter.append(data + NEW_LINE_SEPARATOR);
            } else {
                if (data.length() > 0) {
                    fileWriter.append(data);
                    if (!data.substring(data.length() - 1).equals(COMMA_DELIMITER)) {
                        fileWriter.append(COMMA_DELIMITER);
                    }
                }
            }
            // clear internal buffer after writing
            dataBuffer.setLength(0);
        } catch (Exception e) {
            logger.error("Error in Csv File Writer: " + e, e);
        }
    }

    /**
    * write data to file, one object.  Use concat for list of values.
    * @param data to write.
    * @param isLast is the data going to be at the end of a line.
    */
    public void write(Object data, boolean isLast) {
        if (data != null) {
            dataBuffer.setLength(0);
            dataBuffer.append(data);
        }
        write(isLast);
    }

    /**
    * write the internal buffer to file.
    * By default, its not the last bit of data written to the line.
    */
    public void write() {
        write(false);
    }

    /**
     * write to top of the file only if file is empty.
     * @param header text to be written to top file (usually) such as column names.
     */
    public void writeHeader(String header) {
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
     */
    public static long getDurationInMilliSec(Instant earlier, Instant later) {
        return Duration.between(earlier, later).toMillis();
    }

    public static Instant getInstant() {
        return Instant.now();
    }

    /**
     * Instant now.
     */
    public Instant before() {
        before = Instant.now();
        return before;
    }

    /**
     * Duration between last call to before() and now.
     * @param isFirst is this the first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void after(boolean isFirst, boolean log) {
        concat(getDurationInMilliSec(before, Instant.now()), isFirst, log);
    }

    /**
     * Duration between last call to before() and now.
     * No arguments implies arguments of false meaning it's not the first.
     * in a list of values being written and it's not to be written to log yet.
     */
    public void after() {
        after(false, false);
    }

    /**
     * Duration between Instant prior and now.
     * @param prior.
     * @param isFirst is this first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void after(Instant prior, boolean isFirst, boolean log) {
        concat(getDurationInMilliSec(prior, Instant.now()), isFirst, log);
    }

    /**
     * Duration between Instant prior and now.
     * By default it's not the first in a list of values being written and
     * it's not to be written to log yet.
     * @param prior
     */
    public void after(Instant prior) {
        concat(getDurationInMilliSec(prior, Instant.now()));
    }

    /**
     * Build a comma separated list of values to add to file.  Write if log is true.
     * @param data data to write.
     * @param isFirst is this first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void concat(Object data, boolean isFirst, boolean log) {
        if (isFirst) {
            dataBuffer.setLength(0);
        }
        dataBuffer.append(data + COMMA_DELIMITER);
        if (log) {
            write();
        }
    }

    /**
     * By default data is not the first in a list of values being written and
     * it's not to be written to log yet.
     * @param data data to write.
     */
    public void concat(Object data) {
        concat(data, false, false);
    }

    /**
     * Build a comma separated list of values to add to file, at the end of a row.  Write if log is true.
     * @param data data to write.
     * @param isFirst is this first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void concatAtEnd(Object data, boolean isFirst, boolean log) {
        concat(data, false, true);
        write(true);
    }

    /**
     * concatenate date and time to be logged.
     * @param isFirst is this first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void concatDateTime(boolean isFirst, boolean log) {
        concat(getDateTime(), false, false);
    }

    /**
     * concatenate time to be logged.
     * @param isFirst is this first value in a list of values being written.
     * @param log - write to file (true when a concatenated list is ready to write).
     */
    public void concatTime(boolean isFirst, boolean log) {
        concat(getTime(), false, false);
    }
}
