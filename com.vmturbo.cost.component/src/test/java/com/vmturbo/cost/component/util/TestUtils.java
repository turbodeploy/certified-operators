package com.vmturbo.cost.component.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;

/**
 * General test related util methods.
 */
public class TestUtils {
    /**
     * Logging.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Private constructor.
     */
    private TestUtils() {}

    /**
     * Reads the specified template resource file, containing template placeholders. Fills it up
     * with the specified values in the values map, then loads that file content into the specified
     * protobuf builder that is passed in.
     *
     * @param templateFilePath Template resource file.
     * @param valuesMap Map of header name to value.
     * @param builder Protobuf builder to load file contents as.
     * @throws IOException Thrown on IO error.
     */
    public static void loadProtobufBuilder(@Nonnull final String templateFilePath,
            @Nonnull final Map<String, String> valuesMap,
            @Nonnull final Builder builder) throws IOException {
        // Read template file contents to a String.
        final String templateContents = TestUtils.readTxtFile(templateFilePath, TestUtils.class);

        // Make up the substitution string using the values map.
        String resolvedString = new StringSubstitutor(valuesMap).replace(templateContents);

        // Create stream and load it into the input builder.
        final InputStream is = IOUtils.toInputStream(resolvedString, StandardCharsets.UTF_8);
        JsonFormat.parser().merge(new InputStreamReader(is), builder);
    }

    /**
     * Similar to the overridden method, except this will try to read from the specified
     * input stream if non-null.
     *
     * @param resourcePath Path to the json resource file under src/test/resources/.
     * @param builder Instance of the builder to populate with data read from the json file.
     * @param is If non-null, will try to read from here.
     * @return Whether loading of the resource file was successful. Use as assertTrue() in tests.
     */
    public static boolean loadProtobufBuilder(@Nonnull final String resourcePath,
            @Nonnull final Builder builder, @Nullable InputStream is) {
        if (is == null) {
            // Needs to start with a '/', so add if not present.
            final String jsonFilePath = resourcePath.startsWith(File.separator) ? resourcePath
                    : String.format("%s%s", File.separator, resourcePath);
            is = builder.getClass().getResourceAsStream(jsonFilePath);
            if (is == null) {
                return false;
            }
        }
        boolean success = false;
        try {
            JsonFormat.parser().merge(new InputStreamReader(is), builder);
            success = true;
        } catch (IOException ioe) {
            logger.warn("Unable to load protobuf JSON resource {}.", resourcePath, ioe);
        }
        return success;
    }

    /**
     * Util method to get epoch millis from UTC display string.
     *
     * @param utcDisplay Display time, e.g. "2021-01-31T00:00:00".
     * @return Epoch millis.
     */
    public static long getTimeMillis(String utcDisplay) {
        return TimeUtil.localDateTimeToMilli(LocalDateTime.parse(utcDisplay), Clock.systemUTC());
    }

    /**
     * Reads the specified CSV file from resource path.
     *
     * @param csvFilePath Path to CSV file.
     * @param clazz For resource lookup.
     * @return CSVParser that is Iterable and can be used to fetch rows.
     * @throws IOException Thrown on read error.
     */
    @Nullable
    public static CSVParser readCsvFile(@Nonnull final String csvFilePath, @Nonnull final Class<?> clazz)
            throws IOException {
        final InputStream is = clazz.getResourceAsStream(csvFilePath);
        if (is != null) {
            return CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new InputStreamReader(is));
        }
        return null;
    }

    /**
     * Loads up text file as a string.
     *
     * @param txtFilePath File path.
     * @param clazz Class name.
     * @return String with contents of the file.
     * @throws IOException Thrown on read error.
     */
    @Nullable
    public static String readTxtFile(@Nonnull final String txtFilePath, @Nonnull final Class<?> clazz)
            throws IOException {
        final InputStream is = clazz.getResourceAsStream(txtFilePath);
        if (is != null) {
            List<String> lines = IOUtils.readLines(is, Charset.defaultCharset());
            return StringUtils.join(lines, "\n");
        }
        return null;
    }
}
