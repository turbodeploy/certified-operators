package com.vmturbo.platform.analysis.utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class is for internal use to log stats to file.
 *
 * It logs to a file named by the date and market name, so that a market and plan running in parallel
 * preserve data integrity.
 * Supports concurrent writes by multiple threads
 *
 * @author reshmakumar1
 *
 */

public class StatsUtils {

    static final Logger logger = LogManager.getLogger(StatsUtils.class);
    // Delimiters used in CSV file
    private static final String FIELD_SEPARATOR = ",";
    private static final String RECORD_SEPARATOR = "\n";
    private static final String statsFilePrefix = Paths.get(".").toAbsolutePath().normalize()
                    .toString() + File.separator
                                                  + "data"
                                                  //+ File.separator
                                                  //+ "repos"
                                                  + File.separator;

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat simpleDateFormatWithTime =
                                                       new SimpleDateFormat("MM-DD-YY-HH:SS");
    private static final SimpleDateFormat simpleTimeFormat =
                                                           new SimpleDateFormat("h:mm a");
    private String statsFileName;
    private StringBuilder dataBuffer = new StringBuilder();
    private Instant before_;
    private boolean enabled_ = true;

    public StatsUtils(@NonNull String filename, boolean isEnabled) {
        enabled_ = isEnabled;
        if (!(enabled_)) {
            return;
        }
        Path path = Paths.get(statsFilePrefix);
        String prefix = Files.exists(path)
                        ? statsFilePrefix
                        : System.getProperty("user.home")
                          + File.separator
                          + "data"
                          + File.separator;
        // keep current path for unit test, as it will run on build server also
        statsFileName =
                      filename.contains("UnitTest") ? "target" + File.separator + filename + ".csv"
                                      : prefix + filename + ".csv";
        File file = new File(statsFileName);
        file.setExecutable(true, false);
        file.setReadable(true, false);
        file.setWritable(true, false);
        if (!file.canWrite()) {
            // Even though stats writing has been disabled, log warning when called from unit test
            logger.warn("Could not set write permission for M2 Stats File");
            enabled_ = false;
            return;
        }
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        logger.info("stats file path:" + statsFileName);
    }

    /**
     * Returns the stats are enabled.
     *
     * Stats are written to file daily if enabled is true.
     */
    public boolean isEnabled() {
        return enabled_;
    }

    /**
     * Sets the <b>enableStats<b> flag.
     *
     */
    private void setEnabled(boolean enabled) {
        this.enabled_ = enabled;
    }

    /**
     * Return the internal data buffer.
     *
     *
     */
    public String getdata() {
        return dataBuffer.toString();
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
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(statsFileName, true))) {
            // append data to file
            String data = dataBuffer.toString();
            if (isLast) {
                bufferedWriter.append(data + RECORD_SEPARATOR);
            } else {
                if (data.length() > 0) {
                    bufferedWriter.append(data);
                    if (!data.substring(data.length() - 1).equals(FIELD_SEPARATOR)) {
                        bufferedWriter.append(FIELD_SEPARATOR);
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
     * The Instant at the beginning of a duration to be computed later.
     *
     * It returns a value, so that it can be saved for later use, say in a local variable, as every
     * call to this method will reset before_.
     * All duration calculations calculate duration from the last call to before().  Suppose we
     * wanted to calculate durations between events A and B  and also B and C  and A and C.  Then
     * the Instant before() called for event B would overwrite the value of before_ at event A.
     * Hence the return value, so that the value of before_ at A can be preserved by the caller if
     * required.
     */
    public Instant before() {
        before_ = Instant.now();
        return before_;
    }

    /**
     * Duration between last call to before() and now.
     *
     * @param isFirst is this the first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void after(boolean isFirst, boolean doFlush) {
        append(getDurationInMilliSec(before_, Instant.now()), isFirst, doFlush);
    }

    /**
     * Duration between last call to before() and now.
     *
     * No arguments implies both arguments are default false, meaning it's not the first
     * in a list of values to be written, and it's not ready to be written to file yet.
     */
    public void after() {
        after(false, false);
    }

    /**
     * Duration between Instant prior and now.
     *
     * @param isFirst is this first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void after(@NonNull Instant prior, boolean isFirst, boolean doFlush) {
        append(getDurationInMilliSec(prior, Instant.now()), isFirst, doFlush);
    }

    /**
     * Duration between Instant prior and now.
     * By default it's not the first in a list of values to be written and
     * it's not ready to be written to file yet.
     */
    public void after(@NonNull Instant prior) {
        append(getDurationInMilliSec(prior, Instant.now()));
    }

    /**
     * Build a comma separated list of values to add to file.  Write if doFlush is true.
     *
     * @param data data to write.
     * @param isFirst is this first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void append(@NonNull Object data, boolean isFirst, boolean doFlush) {
        if (!isEnabled()) {
            return;
        }
        if (isFirst) {
            dataBuffer.setLength(0);
        }
        dataBuffer.append(data + FIELD_SEPARATOR);
        if (doFlush) {
            flush();
        }
    }

    /**
     * By default data is not the first in a list of values to be written and
     * it's not ready to be written to file yet.
     *
     * @param data data to write.
     */
    public void append(@NonNull Object data) {
        append(data, false, false);
    }

    /**
     * Build a comma separated list of values to add to file, at the end of a row.
     * Write if doFlush is true.
     *
     * @param data data to write.
     * @param isFirst is this first value in a list of values  to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendAtRowEnd(@NonNull Object data, boolean isFirst, boolean doFlush) {
        append(data, false, doFlush);
        flush(true);
    }

    /**
     * Concatenate date and time to be logged.
     *
     * @param isFirst is this first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendDateTime(boolean isFirst, boolean doFlush) {
        append(getDateTime(), false, doFlush);
    }

    /**
     * concatenate time to be logged.
     *
     * @param isFirst is this first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendTime(boolean isFirst, boolean doFlush) {
        append(getTime(), false, doFlush);
    }

    /**
     * concatenate date to be logged.
     *
     * @param isFirst is this first value in a list of values to be written.
     * @param doFlush - write to file (true when a concatenated list is ready to write).
     */
    public void appendDate(boolean isFirst, boolean doFlush) {
        append(getDate(), false, doFlush);
    }

    /**
     * Split String into tokens based on a specified delimiter.
     *
     * @param str  String to split.
     * @param delim The delimiter.
     */
    public static String[] getTokens(String str, String delim){
        StringTokenizer stok = new StringTokenizer(str, delim);
        String tokens[] = new String[stok.countTokens()];
        for(int i=0; i<tokens.length; i++){
            tokens[i] = stok.nextToken();
        }
        return tokens;
    }

}
