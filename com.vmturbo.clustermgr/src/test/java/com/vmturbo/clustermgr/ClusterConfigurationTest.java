package com.vmturbo.clustermgr;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.clustermgr.api.ClusterConfiguration;

public class ClusterConfigurationTest {

    private final Logger logger = LogManager.getLogger();

    @Test
    public void jacksonDeserializationTest() throws Exception {
        // Arrange
        InputStream clusterConfigJson = getClass().getClassLoader().getResourceAsStream("clusterConfigurationTest.json");
        // Act
        ClusterConfiguration
                testValue = new ObjectMapper().readValue(clusterConfigJson, ClusterConfiguration.class);
        // Assert
        // test the "defaults" section first
        assertThat(testValue.getDefaults().getComponentNames().size(), is(2));
        String[] expectedComponentTypes = {"c1", "c2"};
        String[] expectedPropNames = {"prop1", "prop2"};
        assertThat(testValue.getDefaults().getComponentNames(), containsInAnyOrder(expectedComponentTypes));
        assertThat(testValue.getDefaults().getComponents().size(), is(2));
        assertThat(testValue.getDefaults().getComponents().get("c1").size(), is(2));
        assertThat(testValue.getDefaults().getComponents().get("c1").keySet(), containsInAnyOrder(expectedPropNames));
        assertThat(testValue.getDefaults().getComponents().get("c1").get("prop1"), is("val1"));
        assertThat(testValue.getDefaults().getComponents().get("c1").get("prop2"), is("val2"));
        assertThat(testValue.getDefaults().getComponents().get("c2").size(), is(1));
        assertThat(testValue.getDefaults().getComponents().get("c2").get("prop3"), is("val3"));

        // now test the "instances" section
        String[] expectedComponentIds = {"c1_1", "c2_1"};
        assertThat(testValue.getInstances().size(), is(2));
        assertThat(testValue.getInstances().keySet(), containsInAnyOrder(expectedComponentIds));
        assertThat(testValue.getInstances().get("c1_1").getComponentType(), is("c1"));
        assertThat(testValue.getInstances().get("c1_1").getProperties().size(), is(2));
        String[] expectedC1PropertyKeys = {"prop1", "prop2"};
        assertThat(testValue.getInstances().get("c1_1").getProperties().keySet(), containsInAnyOrder(expectedC1PropertyKeys));
        assertThat(testValue.getInstances().get("c1_1").getProperties().get("prop1"), is("val1"));
        assertThat(testValue.getInstances().get("c1_1").getProperties().get("prop2"), is("val2"));

        assertThat(testValue.getInstances().get("c2_1").getComponentType(), is("c2"));
        assertThat(testValue.getInstances().get("c2_1").getProperties().size(), is(1));
        assertThat(testValue.getInstances().get("c2_1").getProperties().get("prop3"), is("val3"));


    }

    @Test
    public void jacksonSerializationTest() throws Exception {
        // Arrange
        InputStream clusterConfigJson = getClass().getClassLoader().getResourceAsStream("clusterConfigurationTest.json");
        ClusterConfiguration testValue = new ObjectMapper().readValue(clusterConfigJson, ClusterConfiguration.class);
        // Act
        ObjectMapper mapper = new ObjectMapper();
        String serialized = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(testValue);
        logger.info("serialized" + serialized);
        // Assert
        // test the "defaults" section first


    }

}