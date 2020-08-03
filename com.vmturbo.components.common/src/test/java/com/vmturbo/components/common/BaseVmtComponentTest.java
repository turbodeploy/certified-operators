package com.vmturbo.components.common;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

/**
 * Test cases for {@link BaseVmtComponent}.
 */
public class BaseVmtComponentTest {
    public static final String DB_PORT = "dbPort";
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setup() {
        System.clearProperty(DB_PORT);
        environmentVariables.clear(DB_PORT);
    }

    @Test
    public void testIsOverriddenPositiveSystemProperty() {
        System.setProperty(DB_PORT, "3307");
        assertTrue(BaseVmtComponent.isOverridden(DB_PORT));
        assertEquals("3307", System.getProperty(DB_PORT));
    }

    @Test
    public void testIsOverriddenPositiveEnvVariable() {
        environmentVariables.set(DB_PORT, "3307");
        assertTrue(BaseVmtComponent.isOverridden(DB_PORT));
        assertEquals("3307", System.getenv(DB_PORT));
    }

    @Test
    public void testIsOverriddenNegative() {
        assertFalse(BaseVmtComponent.isOverridden(DB_PORT));
    }
}
