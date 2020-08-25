package com.vmturbo.testbases.flyway;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.http.impl.client.HttpClients;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * Base class to perform migration integration testing in component modules.
 *
 * <p>Each component that makes use of Flyway migrations should extend this class with its own
 * test class, overriding methods as needed to properly specify the locations of migration files
 * and of a whitelist properties file, if any. Normally, only @link{{@link #getRelativeModulePath()}}
 * needs to be overridden, as the defaults for other methods represent our standard practice.</p>
 */
public class FlywayMigrationIntegrityTestBase {

    protected static FlywayMigrationIntegrityChecker checker;

    private static final ImmutableList<String> DEFAULT_SOURCE_DIRS = ImmutableList.of(
            "src/main/java", "src/main/resources"
    );

    private static final ImmutableList<String> DEFAULT_MIGRATION_LOCATIONS = ImmutableList.of(
            "db/migration"
    );

    /**
     * This allows integrity violations to be collected and listed at the end of test execution,
     * without stopping any test on its first failure.
     *
     * <p>Failures that are not explicitly recorded with this collector will behave normally.</p>
     */
    @Rule
    public ErrorCollector errors = new ErrorCollector();

    /**
     * Set up for tests, including setting up for git operations.
     *
     * @throws IOException     on failure
     * @throws GitAPIException if there are git problems
     */
    @Before
    public void beforeClass() throws IOException, GitAPIException {
        if (checker == null) {
            checker = new FlywayMigrationIntegrityChecker(
                    getRelativeModulePath(),
                    getRelativeSourceDirs(),
                    getRelativeMigrationLocations(),
                    getWhitelistResourceUrl(),
                    getGitlabApi());
        }
    }

    /**
     * Get the path from working tree root to the root directory of the module being tested.
     *
     * <p>By default, we retrieve the package name of the test class. Placing test classes that
     * extend this class can therefore work with no customization if they are placed in a package
     * that matches the module name.</p>
     *
     * @return path from working tree root to module root
     */
    protected String getRelativeModulePath() {
        return getClass().getPackage().getName();
    }

    protected URL getWhitelistResourceUrl() {
        return getClass().getResource("/flywayMigrationWhitelist.yaml");
    }

    protected GitlabApi getGitlabApi() {
        return new GitlabApi(GitlabApi.TURBO_GITLAB_HOST, GitlabApi.XL_PROJECT_PATH,
                HttpClients::createDefault);
    }

    protected List<String> getRelativeSourceDirs() {
        return DEFAULT_SOURCE_DIRS;
    }

    protected ImmutableList<String> getRelativeMigrationLocations() {
        return DEFAULT_MIGRATION_LOCATIONS;
    }

    /**
     * Drop static reference to the checker instance, which itself holds instances to some
     * potentially large git-related objects that we don't want pinned to the heap while other
     * tests are running.
     */
    @AfterClass
    public static void afterClass() {
        checker = null;
    }

    /**
     * Test for colliding migrations - two migration files for the same migration version.
     */
    @Test
    public void checkNoCollidingMigrations() {
        checker.getCollidingMigrations().forEach(errors::addError);
    }

    /**
     * Test for changed migrations - migrations appearing in both the base commit and the working
     * tree whose migration files are not identical.
     *
     * @throws IOException if there's a problem retrieving the content of migration files
     */
    @Test
    public void testMigrationsHaveNotChanged() throws IOException {
        checker.getChangedMigrations().forEach(errors::addError);
    }

    /**
     * Test for inserted migrations - migrations in the working tree that have a version that sorts
     * prior to the maximum migration version appearing in the base commit.
     */
    @Test
    public void testMigrationsHaveNoInsertions() {
        checker.getMigrationInsertions().forEach(errors::addError);
    }

    /**
     * Test for migration deletions - migrations in the base commit that no longer exist in the
     * working tree.
     */
    @Test
    public void testMigrationsHaveNoDeletions() {
        checker.getMigrationDeletions().forEach(errors::addError);
    }
}
