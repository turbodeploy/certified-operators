package com.vmturbo.components.common;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Pattern;

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

    /**
     * The test setup contains 5 files (all in the config resource): a default properties
     * file that is supposed to be loaded first, another properties file that adds a new
     * property and overrides one property from default, a text file and then a properties
     * file with a long property and a text file with a line break.
     *
     * @throws URISyntaxException not expected to happen
     * @throws IOException not expected to happen
     */
    @Test
    public void testLoadConfigurationProperties() throws URISyntaxException, IOException {
        Properties properties = BaseVmtComponent.loadConfigurationProperties();
        BaseVmtComponent.fetchLocalConfigurationProperties(properties);
        // "key1" is defined in component_default.properties and overrides in override.properties
        assertEquals("override", BaseVmtComponent.getConfigurationProperty("key1"));
        // "key2" is defined in component_default.properties
        assertEquals("value2", BaseVmtComponent.getConfigurationProperty("key2"));
        // "key3" is defined in overide.properties
        assertEquals("value3", BaseVmtComponent.getConfigurationProperty("key3"));
        // The content of the file test.txt is "text-value"
        assertEquals("text-value", BaseVmtComponent.getConfigurationProperty("test.txt"));
        // test2.txt contains two lines
        Pattern multiline = Pattern.compile("line1\\Rline2");
        assertTrue(multiline.matcher(BaseVmtComponent.getConfigurationProperty("test2.txt")).matches());
    }
}
