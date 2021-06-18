package com.vmturbo.api.component.communication;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.sql.utils.TestDBMigrationChecksums;

/**
 * Validates the checksums for the DB migration files in this project against
 * those recorded in the checksum properties file.
 */
public class DBMigrationScriptsTest extends TestDBMigrationChecksums {

    /**
     * Compares the files in this project with their recorded checksums.
     *
     * @throws Exception If there was any properly reading the migration file, or checksums,
     *     or if any checksums did not match
     */
    @Test
    public void testMigrations() throws Exception {
        final File migrationDir = new File("src/main/resources/db/api/migration");
        Assert.assertTrue("Migration directory must exist and must be a directory.",
                migrationDir.exists() && migrationDir.isDirectory());

        final File checksumFile = new File("src/test/resources/db-migration-checksums.properties");
        assertTrue("Checksum record file must be available",
                checksumFile.exists() && checksumFile.isFile());

        ensureAllFilesUnchanged(migrationDir, checksumFile);
    }

}
