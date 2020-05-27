package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;

/**
 * This utility class contains information related to filename for discovery dumping.
 * A discovery dumping filename should contain a "sanitized" version of the target name
 * (i.e., a version without illegal characters for a filename), a timestamp, the type of the
 * discovery, an extension depending on whether it is a binary file or a text file, and
 * possibly a gzip extension.
 *
 * The main purpose of the class is to extract information from a filename, create a filename
 * from its components, and ensure that all these computations are not repeated.
 *
 * A single class instance can be used to produce file names for text or binary, and gzipped or not.
 *
 * This class was largely copied from the class with the same name in Classic.
 */
public class DiscoveryDumpFilename implements Comparable<DiscoveryDumpFilename> {

    private static final String L4ZIP_FILE_SUFFIX = ".l4z";

    /**
     * This is the information that a discovery dump name should contain:
     * the sanitized name of the target, the time the discovery took place,
     * and the type of the discovery.
     */
    private final String sanitizedTargetName;
    private final Date timestamp;
    private final DiscoveryType discoveryType;

    /**
     * These fields store the actual filenames so that we only compute them once.
     */
    private final String filenameBinary;
    private final String filenameText;

    /**
     * The date format used for timestamps
     */
    public final static DateFormat dateFormat =
        new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS");

    /**
     * extensions of binary and text dump files resp.
     */
    private static final String BINARY_EXTENSION = "dto";
    private static final String TEXT_EXTENSION = "txt";

    /**
     * Creates a valid object given a sanitized target name, a timestamp, and a discovery type.
     * Calculates the values of the two filenames that correspond to this discovery dump
     * (binary and text format) and stores them, to avoid calculating them again in the future.
     *
     * @param sanitizedTargetName the sanitized target name.
     * @param timestamp the timestamp.
     * @param discoveryType the discovery type.
     * @throws IllegalArgumentException if target name is empty.
     */
    public DiscoveryDumpFilename(
          @Nonnull String sanitizedTargetName, @Nonnull Date timestamp,
          @Nonnull DiscoveryType discoveryType)
          throws IllegalArgumentException {
        if (Objects.requireNonNull(sanitizedTargetName).isEmpty()) {
            throw new IllegalArgumentException("Empty target name");
        }
        this.sanitizedTargetName = sanitizedTargetName;
        this.timestamp = (Date)Objects.requireNonNull(timestamp).clone();
        this.discoveryType = Objects.requireNonNull(discoveryType);
        final String genericFileName =
            sanitizedTargetName + "-" + dateFormat.format(timestamp) + "-" +
            discoveryType.name() + ".";
        filenameBinary = genericFileName + BINARY_EXTENSION;
        filenameText = genericFileName + TEXT_EXTENSION;
    }

    @Nonnull
    public String getSanitizedTargetName() { return sanitizedTargetName; }

    @Nonnull
    public Date getTimestamp() {
        return (Date)timestamp.clone();
    }

    @Nonnull
    public DiscoveryType getDiscoveryType() {
        return discoveryType;
    }

    /**
     * This is the function to sanitize a target name: digits, latin characters, and the dot
     * are allowed.  All other characters are replaced by underscores.
     *
     * @param s the input string.
     * @return the sanitized version of the input string.
     */
    @Nonnull
    public static String sanitize(String s) {
        return Objects.requireNonNull(s).replaceAll("[^a-zA-Z0-9\\.]", "_");
    }

    /**
     * Returns {@link File} object associated with the current object and a given dumping directory.
     *
     * @param dumpDirectory the directory in which discovery dumps are written.
     * @param isText decides whether to return the text or the binary format file.
     * @param isL4ipped whether the file will be gzipped, and therefore have a suffix appended to the name
     * @return a {@link File} object that corresponds to the discovery file created by the information
     *         in the current object, given the dumping directory and the format.
     */
    @Nonnull
    public File getFile(@Nonnull File dumpDirectory, boolean isText, boolean isL4ipped) {
        String fileName = isText ? filenameText : filenameBinary;
        if (isL4ipped) {
            fileName += L4ZIP_FILE_SUFFIX;
        }
        return
            new File(Objects.requireNonNull(dumpDirectory), fileName);
    }

    /**
     * This function parses a filename and creates the corresponding
     * {@link DiscoveryDumpFilename} object or {@code null} if the input is not a valid
     * discovery dump filename.
     *
     * If there are three hyphen-separated sections in the filename, the function assumes
     * that the first one has been 'sanitized', i.e., only contains digits, letters, dots,
     * and underscores.
     *
     * @param filename the discovery dump filename.
     * @return {@link DiscoveryDumpFilename} object corresponding to {@code filename}.
     */
    @Nullable
    public static DiscoveryDumpFilename parse(@Nonnull String filename) {
        // split into sections
        final String[] filenameSections = Objects.requireNonNull(filename).split("-");
        if (filenameSections.length != 3 || filenameSections[0].isEmpty()) {
            return null;
        }

        // retrieve sanitized target name
        final String sanitizedTargetName = filenameSections[0];

        // retrieve timestamp
        final Date timeStamp;
        try {
            timeStamp = dateFormat.parse(filenameSections[1]);
        } catch (ParseException e) {
            // filename does not have a valid timestamp
            return null;
        }

        // split third part into discovery type and extension, after stripping possible gzipe extension
        String lastFilenameSection = filenameSections[2];
        if (lastFilenameSection.endsWith(L4ZIP_FILE_SUFFIX)) {
            lastFilenameSection = lastFilenameSection.substring(0, lastFilenameSection.length() - L4ZIP_FILE_SUFFIX.length());
        }
        final String[] splitType = lastFilenameSection.split("\\.");
        if (splitType.length != 2) {
            return null;
        }

        // retrieve discovery type
        final DiscoveryType discoveryType;
        try {
            discoveryType = DiscoveryType.valueOf(splitType[0]);
        } catch (Exception e) {
            // filename does not have a valid discovery type
            return null;
        }

        // check extension: must be either dto or txt
        if (!splitType[1].equals(BINARY_EXTENSION) && !splitType[1].equals(TEXT_EXTENSION)) {
            return null;
        }

        // create new DiscoveryDumpFilename object
        return new DiscoveryDumpFilename(sanitizedTargetName, timeStamp, discoveryType);
    }

    /**
     * Comparison between dump file names is chronological.
     *
     * @param other dump file name to compare to.
     * @return chronological comparison.
     */
    @Override
    public int compareTo(@Nonnull DiscoveryDumpFilename other) {
       return timestamp.compareTo(other.getTimestamp());
    }
}
