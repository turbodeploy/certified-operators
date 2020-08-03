package com.vmturbo.testbases.flyway;

/**
 * This test class just ensures that the {@link FlywayMigrationIntegrityTestBase} class can run its
 * tests.
 *
 * <p>We don't actually have any migrations (at least not in the normal locations), so this
 * shouldn't find any violations.</p>
 */
public class MigrationIntegrityTest extends FlywayMigrationIntegrityTestBase {
}
