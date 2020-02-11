package com.vmturbo.components.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.core.env.PropertySource;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;

/**
 * Test fetching external configuration properties from (1) properties.yaml file, and
 * (2) external .properties sources.
 */
public class ExternalPropertiesTest {

    private static final String COMPONENT_TYPE = "test-component";
    private static final String GOOD_TEST_YAML_FILE_PATH = "configmap/sample_properties.yaml";
    private static final String BAD_TEST_YAML_FILE_PATH = "other-properties/doesnt-exist.yaml";
    private static final String GOOD_OTHER_PROPERTIES_RESOURCE = "other-properties";

    /**
     * Test reading properties.yaml file and merging the different sections to return
     * a single PropertiesSource. The sections in increasing priority order are:
     * <ol>
     *     <li>defaultProperties: global:
     *     <li>defauProperties: [component-type]:
     *     <li>customProperties: global: </li>
     *     <li>customProperties: [component-type]: </li>
     * </ol>
     * with a property key/value defined in a lower priority section overridden by
     * the same property key in a higher priority section.
     *
     * <p>Note that the underlying ConfigMapPropertiesReader is more fully tested separately.
     *
     * @throws ContextConfigurationException if there is a semantic error in the configuration file
     */
    @Test
    public void testPropertiesYamlReaderSuccess() throws ContextConfigurationException {
        // act
        final PropertySource<?> propertiesSource = BaseVmtComponent.fetchConfigurationProperties(
            COMPONENT_TYPE, GOOD_TEST_YAML_FILE_PATH);
        // assert
        assertThat(propertiesSource.getProperty("foo"), equalTo("123"));
        assertThat(propertiesSource.getProperty("bar"), equalTo("789"));
        assertThat(propertiesSource.getProperty("xyz"), equalTo("999"));
        assertThat(propertiesSource.getProperty("new"), equalTo("this is new"));
        assertThat(propertiesSource.getProperty("unused"), equalTo(null));
    }

    /**
     * Check that an invalid file path to "properties.yaml" is caught.
     *
     * @throws ContextConfigurationException because of the invalid file path
     */
    @Test(expected = ContextConfigurationException.class)
    public void testPropertiesYamlReaderMissingFile() throws ContextConfigurationException {
        // act
        BaseVmtComponent.fetchConfigurationProperties(COMPONENT_TYPE, BAD_TEST_YAML_FILE_PATH);
        // assert
        fail("Should never reach here - exception should have been thrown");
    }

    /**
     * Test reading "other" properties files. This process scans all the files in the target
     * folder, handling the two types of files differently:
     * <ol>
     *     <li>a ".properties" file is read as such - the individual key/value pairs are
     *     returned as PropertySource key/value pairs
     *     <li>any other file is returned in the PropertySource using the filename as the key
     *     and the contents of the file, concatenated into a single String (perhaps with newline
     *     characters) as the value
     * </ol>

     * @throws ContextConfigurationException if there is an error reading the properties files
     */
    @Test
    public void testOtherPropertiesReader() throws ContextConfigurationException {
        // act
        final PropertySource<?> propertiesSource = BaseVmtComponent.fetchOtherProperties(
            GOOD_OTHER_PROPERTIES_RESOURCE);
        // assert
        assertThat(propertiesSource.getProperty("key1"), equalTo("override"));
        assertThat(propertiesSource.getProperty("key3"), equalTo("value3"));
        assertThat(propertiesSource.getProperty("long.line"),
            equalTo("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"));
        // configuration .txt file - file-name -> file-contents
        assertThat(propertiesSource.getProperty("test.txt"), equalTo("text-value"));
        // multi-line .txt file
        final String property = (String)propertiesSource.getProperty("test2.txt");
        assertThat(property, equalTo("line1" + System.lineSeparator() + "line2"));
    }
}
