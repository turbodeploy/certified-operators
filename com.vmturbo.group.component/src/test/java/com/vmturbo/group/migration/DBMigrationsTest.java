package com.vmturbo.group.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

/**
 * Ensures that all DB migration files have a recorded checksum. Also that no changes are
 * made to a migration file (not even comment changes) after it has been committed and
 * pushed to a public branch
 */
public class DBMigrationsTest {

    private final Logger logger = LogManager.getLogger(DBMigrationsTest.class);

    /**
     * Ensure all files in the migration directory have a checksum recorded, and that the
     * file has not changed from that checksum.
     *
     * @throws Exception is any error occurs
     */
    @Test
    public void testAllFilesUnchanged() throws Exception {
        final File migrationDir = new File("src/main/resources/db/migration");
        assertTrue("Migration directory must exist and must be a directory.",
                   migrationDir.exists() && migrationDir.isDirectory());

        final File[] fileList = migrationDir.listFiles();
        assertNotNull("List of DB migration files must be available", fileList);
        Arrays.sort(fileList, Comparator.comparing(File::getName));

        // Dump values before the test, in case anyone needs this
        // in a format suitable to cut-and-paste into this file.
        logger.debug("Discovered files and checksums");
        for (File file: fileList) {
            logger.debug("{}={}", file.getName(), computeChecksum(file));
        }

        final File checksumFile = new File("src/test/resources/db-migration-checksums.properties");
        assertTrue("Checksum record file must be available",
                   checksumFile.exists() && checksumFile.isFile());
        final Properties checksums = new Properties();
        try (FileReader reader = new FileReader(checksumFile)) {
            checksums.load(reader);
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
     * @throws Exception is any error occurs
     */
    private void validatedMigrationFile(File file, Properties checksums) throws Exception {
        final String key = file.getName();
        final String checksum = computeChecksum(file);

        assertTrue("File " + key + " must have a recorded checksum. " + "Use " + checksum
                + " if you want to add it", checksums.containsKey(key));

        assertEquals("The checksum for the file " + key + " does not match the one recorded. "
                        + "Migration files must never change once published (pushed). Create a new migration file instead.",
                checksums.get(key), checksum);
    }

    /**
     * Computes the checksum for a given file. The checksum algorithm used is not significant.
     *
     * @param file The file for which the checksum needs to be computed.
     *
     * @return String version of the checksum.
     *
     * @throws Exception is any error occurs
     */
    private String computeChecksum(File file) throws Exception {
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] block = new byte[4096];
            int length;

            while ((length = in.read(block)) > 0) {
                digest.update(block, 0, length);
            }

            return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();
        }
    }
}
