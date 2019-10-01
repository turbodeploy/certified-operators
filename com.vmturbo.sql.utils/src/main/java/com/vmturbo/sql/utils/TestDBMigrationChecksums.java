package com.vmturbo.sql.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ensures that all DB migration files have a recorded checksum. Also that no changes are
 * made to a migration file (not even comment changes) after it has beeØn committed and
 * pushed to a public branch.
 *
 * <p>This class contain all the functionality to perform the tests, and depends on subclasses
 * in various components to configure and trigger the tests.</p>
 */
public abstract class TestDBMigrationChecksums {

    private final Logger logger = LogManager.getLogger(TestDBMigrationChecksums.class);

    /**
     * Ensure all files in the migration directory have a checksum recorded, and that the
     * file has not changed from that checksum.
     *
     * @param migrationDir Directory in which the DB migration files exist.
     * @param checksumProperties  Properties file containing checksums
     * @throws ChecksumValidationException is any error occurs
     */
    protected void ensureAllFilesUnchanged(@Nonnull File migrationDir,
                                           @Nonnull File checksumProperties) throws ChecksumValidationException {
        final Properties checksums = new Properties();
        try (FileReader reader = new FileReader(checksumProperties)) {
            checksums.load(reader);
        } catch (Exception e) {
            throw new ChecksumValidationException(e);
        }

        ensureAllFilesUnchanged(migrationDir, checksums);
    }

    /**
     * Ensure all files in the migration directory have a checksum recorded, and that the
     * file has not changed from that checksum.
     *
     * @param migrationDir Directory in which the DB migration files exist.
     * @param checksums A map of base filename to checksum, used to compare against discovered files.
     *
     * @throws ChecksumValidationException is any error occurs
     */
    private void ensureAllFilesUnchanged(@Nonnull File migrationDir,
                                           @Nonnull Map<Object, Object> checksums) throws ChecksumValidationException {
        final File[] fileList = migrationDir.listFiles();
        if (fileList == null) {
            throw new ChecksumValidationException("There are not file in the migration directory: " + migrationDir);
        }

        Arrays.sort(fileList, Comparator.comparing(File::getName));

        // Dump values before the test, in case anyone needs this
        // in a format suitable to cut-and-paste into this file.
        logger.debug("Discovered files and checksums");
        for (File file: fileList) {
            logger.info("{}={}", file.getName(), computeChecksum(file));
        }

        for (File file: fileList) {
            logger.info("Testing for unexpected changes in {}", file);
            validatedMigrationFile(file, checksums);
        }
    }

    /**
     * Checks to make sure the file has a known checksum, and that file file
     * matches this checksum.
     *
     * @param file The file to be checked.
     * @param checksums The known checksums to compare against.
     *
     * @throws ChecksumValidationException is any error occurs
     */
    private void validatedMigrationFile(File file, @Nonnull Map<Object, Object> checksums) throws ChecksumValidationException {
        final String key = file.getName();
        final String checksum = computeChecksum(file);

        if (!checksums.containsKey(key)) {
            throw new ChecksumValidationException("File " + key + " must have a recorded checksum. " +
                                  "Use " + checksum + " if you want to add it");
        }

        final Object value = checksums.get(key);
        if (!checksum.equals(value)) {
            throw new ChecksumValidationException(
                "The checksum for the file " + key + " does not match the one recorded. " +
                "Migration files must never change once published (pushed). " +
                "Create a new migration file instead.");
        }
    }

    /**
     * Computes the checksum for a given file. The checksum algorithm used is not significant.
     *
     * @param file The file for which the checksum needs to be computed.
     *
     * @return String version of the checksum.
     *
     * @throws ChecksumValidationException is any error occurs
     */
    private String computeChecksum(File file) throws ChecksumValidationException {
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] block = new byte[4096];
            int length;

            while ((length = in.read(block)) > 0) {
                digest.update(block, 0, length);
            }

            return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();
        } catch (Exception e) {
            throw new ChecksumValidationException(e);
        }
    }

    /**
     * Special checked exception class that is used when the checksum comparison
     * fails in any way.
     */
    protected static class ChecksumValidationException extends Exception {
        /*pkg*/ ChecksumValidationException(Throwable cause) {
            super(cause);
        }

        /*pkg*/ ChecksumValidationException(String message) {
            super(message);
        }
    }
}
