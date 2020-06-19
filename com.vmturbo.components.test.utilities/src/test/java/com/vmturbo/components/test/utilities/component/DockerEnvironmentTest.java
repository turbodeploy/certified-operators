package com.vmturbo.components.test.utilities.component;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.components.test.utilities.component.ComponentCluster.Component;

public class DockerEnvironmentTest {

    final Component component = mock(Component.class);

    final Map<String, Component> components = new ImmutableMap.Builder<String, Component>()
        .put("market", component)
        .build();

    @Test
    public void testGetEnvironmentUsesDefaults() throws Exception {
        when(component.getMemoryLimitMb()).thenReturn(Optional.empty());
        final Map<String, String> environment = DockerEnvironment.getEnvironmentVariables(components);

        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("DEV_JAVA_OPTS"),
            environment.get("DEV_JAVA_OPTS"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("MARKET_PORT"),
            environment.get("MARKET_PORT"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("MARKET_XMX_MB"),
            environment.get("MARKET_XMX_MB"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("MARKET_MEM_LIMIT_MB"),
            environment.get("MARKET_MEM_LIMIT_MB"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("ACTION_ORCHESTRATOR_XMX_MB"),
            environment.get("ACTION_ORCHESTRATOR_XMX_MB"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("ACTION_ORCHESTRATOR_MEM_LIMIT_MB"),
            environment.get("ACTION_ORCHESTRATOR_MEM_LIMIT_MB"));
    }

    @Test
    public void testGetEnvironmentOverridesMemLimit() throws Exception {
        when(component.getMemoryLimitMb()).thenReturn(Optional.of(1024));
        when(component.getSystemProperties()).thenReturn(Collections.emptyMap());
        final Map<String, String> environment = DockerEnvironment.getEnvironmentVariables(components);

        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("DEV_JAVA_OPTS"),
            environment.get("DEV_JAVA_OPTS"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("MARKET_PORT"),
            environment.get("MARKET_PORT"));
        assertEquals("768", environment.get("MARKET_XMX_MB"));
        assertEquals("1024", environment.get("MARKET_MEM_LIMIT_MB"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("ACTION_ORCHESTRATOR_XMX_MB"),
            environment.get("ACTION_ORCHESTRATOR_XMX_MB"));
        assertEquals(DockerEnvironment.ENVIRONMENT_VARIABLES.get("ACTION_ORCHESTRATOR_MEM_LIMIT_MB"),
            environment.get("ACTION_ORCHESTRATOR_MEM_LIMIT_MB"));
        assertEquals("", environment.get("MARKET_SYSTEM_PROPERTIES"));
    }

    @Test
    public void testGetEnvironmentAppliesSystemProperties() throws Exception {
        when(component.getMemoryLimitMb()).thenReturn(Optional.empty());
        when(component.getSystemProperties()).thenReturn(ImmutableMap.of(
            "some.system.property", "foo",
            "other.system.property", "bar"
        ));

        final Map<String, String> environment = DockerEnvironment.getEnvironmentVariables(components);
        assertEquals("-Dsome.system.property=foo -Dother.system.property=bar",
            environment.get("MARKET_SYSTEM_PROPERTIES"));
    }

    /**
     * Verify that we can find the docker-compose.yml and docker-compose.test.yml files
     */
    @Test
    public void testGetComposeFilesPath() {
        // test the DockerEnvironment.getDockerComposeFiles() static method.
        String[] paths = DockerEnvironment.getDockerComposeFiles();
        // we expect two .yml files in the list
        assertEquals(2, paths.length);
        for( String path : paths) {
            assertTrue(path.endsWith(".yml"));
        }
    }
}