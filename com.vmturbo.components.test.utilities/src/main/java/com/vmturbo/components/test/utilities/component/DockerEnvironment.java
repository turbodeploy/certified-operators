package com.vmturbo.components.test.utilities.component;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.DockerMachine.LocalBuilder;
import com.palantir.docker.compose.connection.DockerMachine.RemoteBuilder;

import com.vmturbo.components.test.utilities.component.ComponentCluster.Component;

/**
 * Utilities for managing the docker environment for deployed
 * components. These are a top-level class for better visibility.
 */
public class DockerEnvironment {

    private DockerEnvironment() {}

    private static final String XMX_SUFFIX = "_XMX_MB";

    private static final String MEM_LIMIT_SUFFIX = "_MEM_LIMIT_MB";

    private static final String SYSTEM_PROPERTIES_SUFFIX = "_SYSTEM_PROPERTIES";

    /**
     * The function to get the -Xmx setting to use for a given mem limit.
     * Since the limit (in MB) won't be overly large, it's safe to do multiplication
     * first without fear of overflow.
     */
    private static final Function<Integer, Integer> MEM_LIMIT_TO_XMX = (limit) -> limit * 3 / 4;

    /**
     * These are environment variables that need to be set in the docker-compose.yml file.
     * See the build/.env file for the environment values in a normal dev deployment.
     */
    @VisibleForTesting
    static final ImmutableMap<String, String> ENVIRONMENT_VARIABLES =
        new ImmutableMap.Builder<String, String>()
            .put("DEV_JAVA_OPTS", "-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=n")
            .put("CONSUL_PORT", "8500")
            .put("DB_PORT", "3306")
            .put("AUTH_PORT", "9100")
            .put("AUTH_DEBUG_PORT", "8000")
            .put("CLUSTERMGR_PORT", "8080")
            .put("CLUSTERMGR_DEBUG_PORT", "8000")
            .put("API_HTTP_PORT", "8080")
            .put("API_HTTPS_PORT", "9443")
            .put("API_DEBUG_PORT", "8000")
            .put("MARKET_PORT", "8080")
            .put("MARKET_DEBUG_PORT", "8000")
            .put("ACTION_ORCHESTRATOR_PORT", "8080")
            .put("ACTION_ORCHESTRATOR_DEBUG_PORT", "8000")
            .put("TOPOLOGY_PROCESSOR_PORT", "8080")
            .put("TOPOLOGY_PROCESSOR_DEBUG_PORT", "8000")
            .put("ARANGODB_PORT", "8529")
            .put("ARANGODB_DUMP_PORT", "8599")
            .put("REPOSITORY_PORT", "8080")
            .put("REPOSITORY_DEBUG_PORT", "8000")
            .put("GROUP_PORT", "8080")
            .put("GROUP_DEBUG_PORT", "8000")
            .put("HISTORY_PORT", "8080")
            .put("HISTORY_DEBUG_PORT", "8000")
            .put("PLAN_ORCHESTRATOR_PORT", "8080")
            .put("PLAN_ORCHESTRATOR_DEBUG_PORT", "8000")
            .put("SAMPLE_PORT", "8080")
            .put("SAMPLE_DEBUG_PORT", "8000")
            .put("MEDIATION_VCENTER_PORT", "8080")
            .put("MEDIATION_VCENTER_DEBUG_PORT", "8000")
            .put("MEDIATION_STRESSPROBE_PORT", "8080")
            .put("MEDIATION_STRESSPROBE_DEBUG_PORT", "8000")
            .put("MEDIATION_DELEGATINGPROBE_PORT", "8080")
            .put("MEDIATION_DELEGATINGPROBE_DEBUG_PORT", "33591:8000")
            .put("MEDIATION_AIX_DEBUG_PORT", "8000")
            .put("MEDIATION_VMAX_DEBUG_PORT", "8000")
            .put("MEDIATION_HYPERV_PORT", "8080")
            .put("MEDIATION_HYPERV_DEBUG_PORT", "8000")
            .put("MEDIATION_OPENSTACK_DEBUG_PORT", "8000")
            .put("MEDIATION_COMPELLENT_DEBUG_PORT", "8000")
            // MEMORY LIMITS and XMX Settings
            .put("DB_MEM_LIMIT_MB", "2048")
            .put("AUTH_MEM_LIMIT_MB", "768")
            .put("AUTH_XMX_MB", "512")
            .put("CLUSTERMGR_MEM_LIMIT_MB", "512")
            .put("CLUSTERMGR_XMX_MB", "384")
            .put("API_MEM_LIMIT_MB", "512")
            .put("API_XMX_MB", "384")
            .put("MARKET_XMX_MB", "384")
            .put("MARKET_MEM_LIMIT_MB", "512")
            .put("ACTION_ORCHESTRATOR_XMX_MB", "384")
            .put("ACTION_ORCHESTRATOR_MEM_LIMIT_MB", "512")
            .put("TOPOLOGY_PROCESSOR_XMX_MB", "1024")
            .put("TOPOLOGY_PROCESSOR_MEM_LIMIT_MB", "2048")
            .put("SAMPLE_XMX_MB", "384")
            .put("SAMPLE_MEM_LIMIT_MB", "512")
            .put("REPOSITORY_XMX_MB", "768")
            .put("REPOSITORY_MEM_LIMIT_MB", "1024")
            .put("GROUP_XMX_MB", "384")
            .put("GROUP_MEM_LIMIT_MB", "512")
            .put("HISTORY_XMX_MB", "768")
            .put("HISTORY_MEM_LIMIT_MB", "1024")
            .put("PLAN_ORCHESTRATOR_XMX_MB", "384")
            .put("PLAN_ORCHESTRATOR_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VCENTER_XMX_MB", "384")
            .put("MEDIATION_VCENTER_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HYPERV_XMX_MB", "384")
            .put("MEDIATION_HYPERV_MEM_LIMIT_MB", "512")
            .put("MEDIATION_STRESSPROBE_XMX_MB", "768")
            .put("MEDIATION_STRESSPROBE_MEM_LIMIT_MB", "1024")
            .put("MEDIATION_DELEGATINGPROBE_XMX_MB", "384")
            .put("MEDIATION_DELEGATINGPROBE_MEM_LIMIT_MB", "512")
            // SYSTEM PROPERTIES
            .put("AUTH_SYSTEM_PROPERTIES", "")
            .put("CLUSTERMGR_SYSTEM_PROPERTIES", "")
            .put("API_SYSTEM_PROPERTIES", "")
            .put("MARKET_SYSTEM_PROPERTIES", "")
            .put("ACTION_ORCHESTRATOR_SYSTEM_PROPERTIES", "")
            .put("TOPOLOGY_PROCESSOR_SYSTEM_PROPERTIES", "")
            .put("SAMPLE_SYSTEM_PROPERTIES", "")
            .put("REPOSITORY_SYSTEM_PROPERTIES", "")
            .put("GROUP_SYSTEM_PROPERTIES", "")
            .put("HISTORY_SYSTEM_PROPERTIES", "")
            .put("PLAN_ORCHESTRATOR_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VCENTER_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HYPERV_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_STRESSPROBE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_DELEGATINGPROBE_SYSTEM_PROPERTIES", "")
            .build();

    /**
     * Construct a builder for starting up the specified components running in a docker instance running locally
     * with an appropriately configured environment.
     *
     * @param components The components whose environment variables should be set on the {@link LocalBuilder}.
     *                   Keys should be the component name (ie "market" or "topology-processor").
     *
     * @return An {@link LocalBuilder} instance capable of bringing up the specified containers.
     */
    public static LocalBuilder newLocalMachine(final Map<String, Component> components) {
        LocalBuilder builder = DockerMachine.localMachine();
        getEnvironmentVariables(components).forEach(builder::withAdditionalEnvironmentVariable);
        return builder;
    }

    /**
     * Construct a builder for starting up the specified components running in a docker instance running remotely
     * with an appropriately configured environment.
     *
     * @param components The components whose environment variables should be set on the {@link RemoteBuilder}.
     *                   Keys should be the component name (ie "market" or "topology-processor").
     *
     * @return A {@link RemoteBuilder} instance capable of bringing up the specified containers.
     */
    public static RemoteBuilder newRemoteMachine(final Map<String, Component> components) {
        RemoteBuilder builder = DockerMachine.remoteMachine();
        getEnvironmentVariables(components).forEach(builder::withAdditionalEnvironmentVariable);
        return builder;
    }

    public static String[] getDockerComposeFiles() {
        // See the path anchor file (located in resources/pathAnchor) for an explanation
        // of why we use it.
        final URL resource = ComponentUtils.class.getClassLoader().getResource("pathAnchor");
        Objects.requireNonNull(resource, "Unable to load pathAnchor." +
            "You must have a file called \"pathAnchor\" at ${project}/src/test/resources/");
        try {
            Path path = Paths.get(resource.toURI()).toAbsolutePath();
            // Go up to top-level XL directory.
            // This won't work if the layout of the modules changes!
            Path rootDir = path.getParent().getParent().getParent().getParent();
            // This is a sanity check - it's not foolproof as the code base changes.
            if (!rootDir.endsWith("XL")) {
                throw new IllegalStateException("Going up four levels from pathAnchor didn't " +
                    "take us to the top-level XL folder!");
            }
            final String pathStr = rootDir.toString() + File.separator + "build" + File.separator;
            return new String[]{pathStr + "/docker-compose.yml", pathStr + "/docker-compose.test.yml"};
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Resource from classloader has invalid URL.", e);
        }
    }

    @VisibleForTesting
    static Map<String, String> getEnvironmentVariables(final Map<String, Component> components) {
        final Map<String, String> environmentVariables = new HashMap<>(ENVIRONMENT_VARIABLES);
        components.forEach((name, component) -> {
            applyMemoryLimit(name, component, environmentVariables);
            applySystemProperties(name, component, environmentVariables);
        });
        return environmentVariables;
    }

    private static void applyMemoryLimit(@Nonnull final String componentName,
                                         @Nonnull final Component component,
                                         @Nonnull final Map<String, String> environmentVariables) {
        component.getMemoryLimitMb().ifPresent(limit -> {
            final String componentNamePrefix =componentNamePrefix(componentName);
            final String xmxLimit = componentNamePrefix + XMX_SUFFIX;
            final String memLimit = componentNamePrefix + MEM_LIMIT_SUFFIX;
            environmentVariables.put(memLimit, Integer.toString(limit));
            environmentVariables.put(xmxLimit, Integer.toString(MEM_LIMIT_TO_XMX.apply(limit)));
        });
    }

    private static void applySystemProperties(@Nonnull final String componentName,
                                              @Nonnull final Component component,
                                              @Nonnull final Map<String, String> environmentVariables) {
        if (component.getSystemProperties().isEmpty()) {
            return;
        }

        final String systemPropertiesVariable = componentNamePrefix(componentName) + SYSTEM_PROPERTIES_SUFFIX;
        final String systemPropertiesValues = component.getSystemProperties().entrySet().stream()
            .map(property -> String.format("-D%s=%s", property.getKey(), property.getValue()))
            .collect(Collectors.joining(" "));

        environmentVariables.put(systemPropertiesVariable, systemPropertiesValues);
    }

    /**
     * We rely on the standard that properties are prefixed with the name of the
     * component, with - replaced by _ (because docker environment variables can't
     * have dashes).
     *
     * @param componentName The component name to transform into an environment variable prefix.
     * @return The environment variable prefix version of the component name.
     */
    private static String componentNamePrefix(@Nonnull final String componentName) {
        return StringUtils.upperCase(componentName.replace("-", "_"));
    }
}
