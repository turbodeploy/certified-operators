package com.vmturbo.testbases.flyway;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.vmturbo.testbases.flyway.Whitelist.ViolationType;

/**
 * Tests for {@link Whitelist}.
 */
public class WhitelistTest {

    /**
     * Test that the builder can build a whitelist, and that it then behaves as expected.
     */
    @Test
    public void testWhitelist() {
        final Whitelist whitelist = new Whitelist.Builder()
                .whitelist("v1", ViolationType.CHANGE)
                .whitelist("v2", ViolationType.INSERT)
                .whitelist("v3", ViolationType.DELETE)
                .whitelist("v4", ViolationType.CHANGE, "foo", "bar")
                .build();
        assertTrue(whitelist.isWhitelisted("v1", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v2", ViolationType.INSERT));
        assertTrue(whitelist.isWhitelisted("v3", ViolationType.DELETE));
        assertFalse(whitelist.isWhitelisted("v1", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foo"));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "bar"));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foobar"));
    }

    /**
     * Test that we can load a whitelist from a JSON file and that it behaves as expected.
     *
     * @throws IOException if there's a problem reading the properties file
     */
    @Test
    public void testWithJsonFile() throws IOException {
        final Whitelist whitelist = Whitelist.fromResource(
                getClass().getResource("flywayMigrationWhitelist.json"));
        assertTrue(whitelist.isWhitelisted("v1", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v2", ViolationType.INSERT));
        assertTrue(whitelist.isWhitelisted("v3", ViolationType.DELETE));
        assertFalse(whitelist.isWhitelisted("v1", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foo"));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "bar"));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foobar"));
    }

    /**
     * Test that we can load a whitelist from a YAML file and that it behaves as expected.
     *
     * @throws IOException if there's a problem reading the properties file
     */
    @Test
    public void testWithYamlFile() throws IOException {
        final Whitelist whitelist = Whitelist.fromResource(
                getClass().getResource("flywayMigrationWhitelist.yaml"));
        assertTrue(whitelist.isWhitelisted("v1", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v2", ViolationType.INSERT));
        assertTrue(whitelist.isWhitelisted("v3", ViolationType.DELETE));
        assertFalse(whitelist.isWhitelisted("v1", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.INSERT));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foo"));
        assertTrue(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "bar"));
        assertFalse(whitelist.isWhitelisted("v4", ViolationType.CHANGE, "foobar"));
    }

    /**
     * Test that trying to parse a whitelist resource with an invalid file extension fails.
     *
     * @throws IOException if there's a problem reading the file
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidWhitelist() throws IOException {
        Whitelist.fromResource(getClass().getResource("flywayMigrationWhitelist"));
    }

    /**
     * Test the {@link Whitelist#empty()} method creates an empty whitelist.
     */
    @Test
    public void testEmptyWhitelist() {
        assertTrue(Whitelist.empty().isEmpty());
    }

    /**
     * Test that if we try to build a whitelist from a null properties file, we get an
     * empty whitelist.
     *
     * @throws IOException can't happen in this case
     */
    @Test
    public void testWithNullPropertiesFile() throws IOException {
        assertTrue(Whitelist.fromResource(null).isEmpty());
    }

}
