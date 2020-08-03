package com.vmturbo.components.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

/**
 * Tests for the class to read the "properties.yaml" configuration file and merge the various
 * sections into a single Properties object, to be used to configure a Turbonomic Component.
 */
public class ConfigMapPropertiesReaderTest {

    /**
     * Test that the different fields of a well structured "properties.yaml" file are correctly
     * read and merged into a single Properties object.
     *
     * @throws IOException should never happen
     */
    @Test
    public void testReadConfigMapSuccess() throws IOException {
        // arrange
        // act
        Properties result = ConfigMapPropertiesReader.readConfigMap("test-component",
            "configmap/sample_properties.yaml");
        // assert
        assertEquals("123", result.get("foo"));
        assertEquals("789", result.get("bar"));
        assertEquals("999", result.get("xyz"));
        assertEquals("this is new", result.get("new"));
    }

    /**
     * Test that a FileNotFound exception will result if the file to read does not exist.
     *
     * @throws IOException in this case a FileNotFoundException since the file does not exist
     */
    @Test(expected = FileNotFoundException.class)
    public void testFileNotFoundFailure() throws IOException {
        // arrange
        // act
        ConfigMapPropertiesReader.readConfigMap("test-component", "configmap/no-such-file.yaml");
        // assert
        fail("should never get here");
    }

    /**
     * Test that an IllegalStateException is thrown if the properties.yaml file has the wrong
     * format. In this case a "defaultProperties" section which is a String, not a Map.
     *
     * @throws IOException should not happen
     */
    @Test(expected = ConfigMapPropertiesReader.MapValueExpected.class)
    public void testPropertiesMapInvalid() throws IOException {
        // arrange
        // act
        ConfigMapPropertiesReader.readConfigMap("test-component", "configmap/invalid_default_properties.yaml");
        // assert
        fail("should never get here");
    }
}