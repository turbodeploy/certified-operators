package com.vmturbo.group.migration;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.sql.utils.TestDBMigrationChecksums;

/**
 * Ensures that all DB migration files have a recorded checksum. Also that no changes are
 * made to a migration file (not even comment changes) after it has been committed and
 * pushed to a public branch
 */
public class DBMigrationsTest extends TestDBMigrationChecksums {

    private final Logger logger = LogManager.getLogger(DBMigrationsTest.class);


    /**
     * Compares the files in this project with their recorded checksums.
     *
     * @throws Exception If there was any properly reading the migration file, or checksums,
     *     or if any checksums did not match
     */
    @Test
    public void testMigrations() throws Exception {
        final File migrationDir = new File("src/main/resources/db/migration");
        Assert.assertTrue("Migration directory must exist and must be a directory.",
                migrationDir.exists() && migrationDir.isDirectory());

        final File checksumFile = new File("src/test/resources/db-migration-checksums.properties");
        assertTrue("Checksum record file must be available",
                checksumFile.exists() && checksumFile.isFile());

        ensureAllFilesUnchanged(migrationDir, checksumFile);
    }
}
