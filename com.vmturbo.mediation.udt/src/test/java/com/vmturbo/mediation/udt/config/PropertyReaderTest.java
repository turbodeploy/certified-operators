package com.vmturbo.mediation.udt.config;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;

/**
 * Test class for {@link PropertyReader}.
 */
public class PropertyReaderTest {

    /**
     * The method tests that PropertyReader correctly reads properties from YAML file.
     *
     * @throws ContextConfigurationException - in case of any exception regarding to the file 'properties.yam'
     */
    @Test
    public void testReadingProperties() throws ContextConfigurationException {
        System.setProperty("propertiesYamlPath", "properties.yaml");
        AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        PropertyReader reader = new PropertyReader(context);
        ConnectionProperties connectionProperties = reader.getConnectionProperties();
        Assert.assertEquals("group", connectionProperties.getGroupHost());
        Assert.assertEquals("repository", connectionProperties.getRepositoryHost());
        Assert.assertEquals(9001, connectionProperties.getgRpcPort());
        Assert.assertEquals(300, connectionProperties.getgRpcPingIntervalSeconds());
    }

    /**
     * The method tests that we have a RuntimeException if it cannot read propertu file.
     *
     * @throws ContextConfigurationException - in case of any exception regarding to the file 'properties.yam'
     */
    @Test(expected = ContextConfigurationException.class)
    public void testReadingPropertiesIncorrectPath() throws ContextConfigurationException {
        System.setProperty("propertiesYamlPath", "properties2.yaml");
        new PropertyReader(new AnnotationConfigWebApplicationContext())
                .getConnectionProperties();
    }

}
