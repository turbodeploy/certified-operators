package com.vmturbo.mediation.udt.config;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test class for {@link PropertyConfiguration}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = PropertyConfiguration.class)
public class PropertyConfigurationTest {

    @Autowired
    PropertyConfiguration configuration;

    /**
     * Verify that the component type value is initialized.
     */
    @Test
    public void testComponentTypeValue() {
        Assert.assertEquals("udt", configuration.componentType);
    }
}
