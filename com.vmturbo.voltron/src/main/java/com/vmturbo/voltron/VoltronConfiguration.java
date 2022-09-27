package com.vmturbo.voltron;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.components.common.featureflags.FeatureFlag;
import com.vmturbo.components.common.featureflags.FeatureFlagEnablementStoreBase;

/**
 * Configuration for a Voltron instance. Created via {@link VoltronConfiguration#newBuilder()}.
 */
public class VoltronConfiguration {
    private final Set<Component> components;
    private final String dataPath;
    private final String uxPath;
    private final String swaggerPath;
    private final SwaggerSetup swaggerSetup;
    private final boolean cleanSlate;
    private final boolean useLocalBus;
    private final boolean useInProcessGrpc;
    private final int serverHttpPort;
    private final int serverGrpcPort;
    private final String licensePath;
    private final Map<String, Object> globalPropertyOverrides;
    private final Map<Component, Map<String, Object>> componentPropertyOverrides;

    private VoltronConfiguration(Set<Component> components,
            String dataPath,
            String uxPath,
            String swaggerPath,
            String licensePath,
            boolean cleanSlate,
            boolean useLocalBus,
            boolean useInProcessGrpc,
            int serverHttpPort,
            int serverGrpcPort,
            Map<String, Object> globalPropertyOverrides,
            Map<Component, Map<String, Object>> componentPropertyOverrides,
            SwaggerSetup swaggerSetup) {
        this.components = components;
        this.dataPath = dataPath;
        this.uxPath = uxPath;
        this.swaggerPath = swaggerPath;
        this.licensePath = licensePath;
        this.cleanSlate = cleanSlate;
        this.useLocalBus = useLocalBus;
        this.useInProcessGrpc = useInProcessGrpc;
        this.serverHttpPort = serverHttpPort;
        this.serverGrpcPort = serverGrpcPort;
        this.globalPropertyOverrides = globalPropertyOverrides;
        this.componentPropertyOverrides = componentPropertyOverrides;
        this.swaggerSetup = swaggerSetup;
    }

    /**
     * Create a new configuration builder.
     *
     * @return The builder.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * If true, use an in-process gRPC server to communicate between contexts in Voltron.
     * This is faster, but is less like the "production" deployment which uses the netty-based
     * gRPC server.
     *
     * @return True if using in-process gRPC.
     */
    public boolean isUseInProcessGrpc() {
        return useInProcessGrpc;
    }

    /**
     * If true, use local bus (basically a bunch of queues) to send messages between contexts.
     * This is faster, and allows reuse of the same protobufs across all producers/consumers,
     * but is less like the "production" deployment which uses Kafka.
     *
     * @return True if using local bus.
     */
    public boolean isUseLocalBus() {
        return useLocalBus;
    }

    /**
     * If true, delete all data before and after running Voltron. Useful for tests, or to test
     * initialization-related logic.
     *
     * @return True if clean slate.
     */
    public boolean cleanSlate() {
        return cleanSlate;
    }

    /**
     * Gets data path.
     *
     * @return the data path
     */
    public String getDataPath() {
        return dataPath;
    }

    /**
     * Gets components.
     *
     * @return the components
     */
    public Set<Component> getComponents() {
        return components;
    }

    /**
     * Get property overrides for this component.
     *
     * @param component The component.
     * @return Map of string (property name) to value (property value).
     */
    @Nonnull
    public Map<String, Object> getComponentOverrides(Component component) {
        final Map<String, Object> retMap = new HashMap<>();
        retMap.putAll(globalPropertyOverrides);
        retMap.putAll(componentPropertyOverrides.getOrDefault(component, Collections.emptyMap()));
        return retMap;
    }

    /**
     * Get the HTTP port.
     *
     * @return The port, which can be used to connect to the component's HTTP server.
     */
    public int getServerHttpPort() {
        return serverHttpPort;
    }

    /**
     * Get the gRPC port.
     *
     * @return The port, which can be used to connect to the component's gRPC server.
     */
    public int getServerGrpcPort() {
        return serverGrpcPort;
    }

    /**
     * Gets ux path.
     *
     * @return the ux path
     */
    @Nonnull
    public String getUxPath() {
        return uxPath;
    }

    /**
     * Gets swagger path.
     *
     * @return the swagger path
     */
    @Nullable
    public String getSwaggerPath() {
        return swaggerPath;
    }

    /**
     * Gets swagger setup.
     *
     * @return the swagger setup
     */
    @Nullable
    public SwaggerSetup getSwaggerSetup() {
        return swaggerSetup;
    }

    /**
     * Get the license path in this configuration (should be a path to a license file).
     *
     * @return The license path.
     */
    @Nonnull
    public Optional<String> getLicensePath() {
        return Optional.ofNullable(licensePath);
    }

    /**
     * Return the map of global property overrides.
     *
     * @return global property overrides
     */
    public Map<String, Object> getGlobalPropertyOverrides() {
        return globalPropertyOverrides;
    }

    /**
     * The builder for the configuration.
     */
    public static class Builder {
        private static final int DEFAULT_HHTP_PORT = 8080;
        private static final int DEFAULT_GRPC_PORT = 9001;

        private final Map<String, Object> globalPropertyOverrides = new HashMap<>();

        private final Map<Component, Map<String, Object>> componentPropertyOverrides = new HashMap<>();

        private boolean cleanOnExit = true;

        private String dataPath = null;

        private String uxPath = null;

        private String swaggerPath = null;

        private SwaggerSetup swaggerSetup = null;

        private String licensePath = null;

        private boolean useLocalBus = true;

        private boolean useInProcessGrpc = true;

        private int serverHttpPort = DEFAULT_HHTP_PORT;

        private int serverGrpcPort = DEFAULT_GRPC_PORT;

        private final Set<Component> components = new HashSet<>();

        /**
         * Add all platform components to the configuration.
         *
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder addPlatformComponents() {
            for (PlatformComponent component : PlatformComponent.values()) {
                components.add(component.getComponent());
            }
            return this;
        }

        /**
         * Add a specific platform component to the configuration.
         *
         * @param platformComponent The component.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder addPlatformComponent(PlatformComponent platformComponent) {
            components.add(platformComponent.getComponent());
            return this;
        }

        /**
         * Add a mediation component to the configuration.
         *
         * @param mediationComponent The probe.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder addMediationComponent(MediationComponent mediationComponent) {
            components.add(mediationComponent.getComponent());
            return this;
        }

        /**
         * Set the data path (this is where Voltron will keep private keys, and so on).
         *
         * @param dataPath The data path. Doesn't have to point to an existing folder.
         * @return The builder for method chaining.
         */
        @Nonnull
        public Builder setDataPath(@Nullable String dataPath) {
            this.dataPath = dataPath;
            return this;
        }

        /**
         * Set the UX path (this is where the UI is compiled and served from).
         *
         * @param uxPath Path to the UI.
         * @return The builder, for method chaining.
         */
        public Builder setUxPath(@Nullable String uxPath) {
            if (uxPath != null) {
                this.uxPath = uxPath;
            }
            return this;
        }

        /**
         * Set the swagger path (this is where the swagger-ui resources are  served from).
         *
         * @param swaggerPath Path to the swagger resources.
         * @return The builder, for method chaining.
         * @deprecated use {@link #setSwaggerSetup(SwaggerSetup)} instead
         */
        @Deprecated
        public Builder setExternalSwaggerPath(@Nullable String swaggerPath) {
            if (swaggerPath != null) {
                this.swaggerPath = swaggerPath;
            }
            return this;
        }

        /**
         * Voltron will use the provided swagger setup object to figure out where to copy the
         * swagger files from.
         *
         * @param swaggerSetup describes where the swagger files are.
         * @return The builder, for method chaining.
         */
        public Builder setSwaggerSetup(@Nullable SwaggerSetup swaggerSetup) {
            if (swaggerSetup != null) {
                this.swaggerSetup = swaggerSetup;
            }
            return this;
        }

        private void checkPortAvailable(int port) {
            try (ServerSocket serverSocket = new ServerSocket(port);
                 DatagramSocket datagramSocket = new DatagramSocket(port)) {
                serverSocket.setReuseAddress(true);
                datagramSocket.setReuseAddress(true);
            } catch (IOException e) {
                throw new IllegalArgumentException("Port " + port + " is not available.");
            }
        }

        /**
         * Set the HTTP port Voltron will listen on.
         *
         * @param httpPort The http port.
         * @return The builder, for method chaining.
         * @throws IllegalArgumentException If the port is not available.
         */
        @Nonnull
        public Builder setHttpPort(@Nullable Integer httpPort) {
            if (httpPort != null) {
                checkPortAvailable(httpPort);
                serverHttpPort = httpPort;
            } else {
                serverHttpPort = DEFAULT_HHTP_PORT;
            }
            return this;
        }

        /**
         * Set the gRPC port Voltron will listen on.
         *
         * @param grpcPort The grpc port.
         * @return The builder, for method chaining.
         * @throws IllegalArgumentException If the port is not available.
         */
        @Nonnull
        public Builder setGrpcPort(@Nullable Integer grpcPort) {
            if (grpcPort != null) {
                checkPortAvailable(grpcPort);
                serverGrpcPort = grpcPort;
            } else {
                serverGrpcPort = DEFAULT_GRPC_PORT;
            }
            return this;
        }

        /**
         * Enable/disable using local bus. See {@link VoltronConfiguration#isUseLocalBus()}.
         *
         * @param newUseLocalBus The new value.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder setUseLocalBus(final boolean newUseLocalBus) {
            this.useLocalBus = newUseLocalBus;
            return this;
        }

        /**
         * Enable/disable using in-process gRPC. See: {@link VoltronConfiguration#isUseInProcessGrpc()}.
         *
         * @param newInProcess New value.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder setUseInProcessGrpc(final boolean newInProcess) {
            this.useInProcessGrpc = newInProcess;
            return this;
        }

        /**
         * Set the path to the license file to use to initialize Voltron. Note - this is only
         * necessary if you want to use the external API.
         *
         * @param licensePath The path to the XML license file.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder setLicensePath(final String licensePath) {
            this.licensePath = licensePath;
            return this;
        }

        /**
         * Enable/disable cleanup on exit. See: {@link VoltronConfiguration#cleanSlate()}.
         *
         * @param newCleanOnExit New value.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder setCleanOnExit(boolean newCleanOnExit) {
            this.cleanOnExit = newCleanOnExit;
            return this;
        }

        /**
         * Override a property for a component.
         *
         * @param component The component.
         * @param propName The property name.
         * @param value The property value.
         * @return The builder, for method chaining.
         */
        public Builder componentPropertyOverride(@Nonnull final Component component,
                @Nonnull final String propName,
                @Nonnull final Object value) {
            componentPropertyOverrides.computeIfAbsent(component, k -> new HashMap<>())
                    .put(propName, value);
            return this;
        }

        /**
         * Override a global property.
         *
         * @param propName The property name.
         * @param value The property value.
         * @return The builder, for method chaining.
         */
        public Builder globalPropertyOverride(String propName, Object value) {
            globalPropertyOverrides.put(propName, value);
            return this;
        }

        /**
         * Specify one or more feature flags to be enabled during this Voltron exececution.
         *
         * @param featureFlags feature flags to be enabled
         * @return workbench builder
         */
        public Builder enableFeatureFlag(FeatureFlag... featureFlags) {
            for (final FeatureFlag featureFlag : featureFlags) {
                final String propertyName =
                        FeatureFlagEnablementStoreBase.getConfigPropertyName(featureFlag);
                globalPropertyOverride(propertyName, "true");
            }
            return this;
        }

        /**
         * Remove a component from the configuration.
         *
         * @param component The component to remove.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder removeComponent(Component component) {
            components.remove(component);
            return this;
        }

        /**
         * Adds a component to the configuration.
         *
         * @param component The component to remove.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder addComponent(Component component) {
            components.add(component);
            return this;
        }

        /**
         * Build the configuration.
         *
         * @return The final {@link VoltronConfiguration}.
         */
        public VoltronConfiguration build() {
            if (StringUtils.isEmpty(dataPath)) {
                File createdFolder = null;
                try {
                    createdFolder = File.createTempFile("voltron", "", null);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to create temp folder.", e);
                }
                createdFolder.delete();
                createdFolder.mkdir();
                dataPath = createdFolder.getAbsolutePath();
            }

            if (components.isEmpty()) {
                // By default we will start all platform components and no mediation containers.
                addPlatformComponents();
            }
            return new VoltronConfiguration(components, dataPath, uxPath, swaggerPath,
                    licensePath, cleanOnExit, useLocalBus,
                    useInProcessGrpc, serverHttpPort, serverGrpcPort,
                    globalPropertyOverrides, componentPropertyOverrides, swaggerSetup);
        }
    }

    /**
     * Enumeration of all "platform" components, for use when initializing.
     */
    public enum PlatformComponent {
        /**
         * Cluster manager.
         */
        CLUSTERMGR(Component.CLUSTERMGR),

        /**
         * Auth.
         */
        AUTH(Component.AUTH),

        /**
         * Topology processor.
         */
        TOPOLOGY_PROCESSOR(Component.TOPOLOGY_PROCESSOR),

        /**
         * Market.
         */
        MARKET(Component.MARKET),

        /**
         * Action orchestrator.
         */
        ACTION_ORCHESTRATOR(Component.ACTION_ORCHESTRATOR),

        /**
         * History.
         */
        HISTORY(Component.HISTORY),

        /**
         * Plan orchestrator.
         */
        PLAN_ORCHESTRATOR(Component.PLAN_ORCHESTRATOR),

        /**
         * Cost.
         */
        COST(Component.COST),

        /**
         * Group.
         */
        GROUP(Component.GROUP),

        /**
         * Repository.
         */
        REPOSITORY(Component.REPOSITORY),

        /**
         * Extractor.
         */
        EXTRACTOR(Component.EXTRACTOR),

        /**
         * API.
         */
        API(Component.API);

        private Component component;

        PlatformComponent(Component component) {
            this.component = component;
        }

        /**
         * Gets component.
         *
         * @return the component
         */
        public Component getComponent() {
            return component;
        }

        /**
         * Determine whether a component is a platform component.
         *
         * @param shortName The short name of the component.
         * @return Whether the component is a platform component.
         */
        public static boolean isPlatformComponent(String shortName) {
            for (PlatformComponent platformComponent : PlatformComponent.values()) {
                if (shortName.equalsIgnoreCase(platformComponent.getComponent().getShortName())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Mediation components.
     */
    public enum MediationComponent {

        /**
         * Actionscript.
         */
        MEDIATION_ACTIONSCRIPT(Component.MEDIATION_ACTIONSCRIPT),

        /**
         * ActionStream Kafka Mediation.
         */
        MEDIATION_ACTIONSTREAM_KAFKA(Component.MEDIATION_ACTIONSTREAM_KAFKA),

        /**
         * APM SNMP.
         */
        MEDIATION_APM_SNMP(Component.MEDIATION_APM_SNMP),

        /**
         * APM WIMI.
         */
        MEDIATION_APM_WMI(Component.MEDIATION_APM_WMI),

        /**
         * AppD.
         */
        MEDIATION_APPD(Component.MEDIATION_APPD),

        /**
         * AppInsights.
         */
        MEDIATION_APPINSIGHTS(Component.MEDIATION_APPINSIGHTS),

        /**
         * AWS Billing.
         */
        MEDIATION_AWS_BILLING(Component.MEDIATION_AWS_BILLING),

        /**
         * AWS Cloud Billing.
         */
        MEDIATION_AWS_CLOUD_BILLING(Component.MEDIATION_AWS_CLOUD_BILLING),

        /**
         * AWS.
         */
        MEDIATION_AWS(Component.MEDIATION_AWS),

        /**
         * AWS Cost.
         */
        MEDIATION_AWS_COST(Component.MEDIATION_AWS_COST),

        /**
         * AWS Lambda.
         */
        MEDIATION_AWS_LAMBDA(Component.MEDIATION_AWS_LAMBDA),

        /**
         * Azure.
         */
        MEDIATION_AZURE(Component.MEDIATION_AZURE),

        /**
         * Azure Cost.
         */
        MEDIATION_AZURE_COST(Component.MEDIATION_AZURE_COST),

        /**
         * Azure EA.
         */
        MEDIATION_AZURE_EA(Component.MEDIATION_AZURE_EA),

        /**
         * Azure EA.
         */
        MEDIATION_AZURE_BILLING(Component.MEDIATION_AZURE_BILLING),

        /**
         * Azure Pricing.
         */
        MEDIATION_AZURE_PRICING(Component.MEDIATION_AZURE_PRICING),

        /**
         * Azure SP.
         */
        MEDIATION_AZURE_SP(Component.MEDIATION_AZURE_SP),

        /**
         * Azure Volumes.
         */
        MEDIATION_AZURE_VOLUMES(Component.MEDIATION_AZURE_VOLUMES),

        /**
         * Baremetal.
         */
        MEDIATION_BAREMETAL(Component.MEDIATION_BAREMETAL),

        /**
         * CloudFoundry.
         */
        MEDIATION_CLOUDFOUNDRY(Component.MEDIATION_CLOUDFOUNDRY),

        /**
         * Compellent.
         */
        MEDIATION_COMPELLENT(Component.MEDIATION_COMPELLENT),

        /**
         * Custom data.
         */
        MEDIATION_CUSTOM_DATA(Component.MEDIATION_CUSTOM_DATA),

        /**
         * DB MSSQL.
         */
        MEDIATION_DB_MSSQL(Component.MEDIATION_DB_MSSQL),

        /**
         * DB MYSQL.
         */
        MEDIATION_DB_MYSQL(Component.MEDIATION_DB_MYSQL),

        /**
         * DataDog.
         */
        MEDIATION_DATADOG(Component.MEDIATION_DATADOG),

        /**
         * Delegating probe.
         */
        MEDIATION_DELEGATING_PROBE(Component.MEDIATION_DELEGATING_PROBE),

        /**
         * Dynatrace.
         */
        MEDIATION_DYNATRACE(Component.MEDIATION_DYNATRACE),

        /**
         * Flexera.
         */
        MEDIATION_FLEXERA(Component.MEDIATION_FLEXERA),

        /**
         * GCP Service Account.
         */
        MEDIATION_GCP_SA(Component.MEDIATION_GCP_SA),

        /**
         * GCP Project.
         */
        MEDIATION_GCP_PROJECT(Component.MEDIATION_GCP_PROJECT),

        /**
         * GCP Cost.
         */
        MEDIATION_GCP_COST(Component.MEDIATION_GCP_COST),

        /**
         * GCP Billing.
         */
        MEDIATION_GCP_BILLING(Component.MEDIATION_GCP_BILLING),

        /**
         * HDS.
         */
        MEDIATION_HDS(Component.MEDIATION_HDS),

        /**
         * Horizon.
         */
        MEDIATION_HORIZON(Component.MEDIATION_HORIZON),

        /**
         * HPE3Par.
         */
        MEDIATION_HPE3PAR(Component.MEDIATION_HPE3PAR),

        /**
         * Hyperflex.
         */
        MEDIATION_HYPERFLEX(Component.MEDIATION_HYPERFLEX),

        /**
         * HyperV.
         */
        MEDIATION_HYPERV(Component.MEDIATION_HYPERV),

        /**
         * Intersight Hyperflex.
         */
        MEDIATION_INTERSIGHT_HYPERFLEX(Component.MEDIATION_INTERSIGHT_HYPERFLEX),

        /**
         * Intersight Server.
         */
        MEDIATION_INTERSIGHT_SERVER(Component.MEDIATION_INTERSIGHT_SERVER),

        /**
         * Intersight UCS.
         */
        MEDIATION_INTERSIGHT_UCS(Component.MEDIATION_INTERSIGHT_UCS),

        /**
         * JVM.
         */
        MEDIATION_JVM(Component.MEDIATION_JVM),

        /**
         * NetApp.
         */
        MEDIATION_NETAPP(Component.MEDIATION_NETAPP),

        /**
         * NewRelic.
         */
        MEDIATION_NEWRELIC(Component.MEDIATION_NEWRELIC),

        /**
         * Nutanix.
         */
        MEDIATION_NUTANIX(Component.MEDIATION_NUTANIX),

        /**
         * OneView.
         */
        MEDIATION_ONEVIEW(Component.MEDIATION_ONEVIEW),

        /**
         * Pivotal.
         */
        MEDIATION_PIVOTAL(Component.MEDIATION_PIVOTAL),

        /**
         * Pure.
         */
        MEDIATION_PURE(Component.MEDIATION_PURE),

        /**
         * RHV.
         */
        MEDIATION_RHV(Component.MEDIATION_RHV),

        /**
         * ScaleIO.
         */
        MEDIATION_SCALEIO(Component.MEDIATION_SCALEIO),

        /**
         * ServiceNOW.
         */
        MEDIATION_SERVICENOW(Component.MEDIATION_SERVICENOW),

        /**
         * Storage stress probe.
         */
        MEDIATION_STORAGE_STRESS(Component.MEDIATION_STORAGE_STRESS),

        /**
         * Stress probe.
         */
        MEDIATION_STRESS(Component.MEDIATION_STRESS),

        /**
         * Tanium.
         */
        MEDIATION_TANIUM(Component.MEDIATION_TANIUM),

        /**
         * Terraform.
         */
        MEDIATION_TERRAFORM(Component.MEDIATION_TERRAFORM),

        /**
         * Tomcat.
         */
        MEDIATION_TOMCAT(Component.MEDIATION_TOMCAT),

        /**
         * UCS.
         */
        MEDIATION_UCS(Component.MEDIATION_UCS),

        /**
         * UCS Director.
         */
        MEDIATION_UCS_DIRECTOR(Component.MEDIATION_UCS_DIRECTOR),

        /**
         * Util probe.
         */
        MEDIATION_UTIL_PROBE(Component.MEDIATION_UTIL_PROBE),

        /**
         * VCD.
         */
        MEDIATION_VCD(Component.MEDIATION_VCD),

        /**
         * VC Browsing.
         */
        MEDIATION_VC_BROWSING(Component.MEDIATION_VC_BROWSING),

        /**
         * VC.
         */
        MEDIATION_VC(Component.MEDIATION_VC),

        /**
         * VMax.
         */
        MEDIATION_VMAX(Component.MEDIATION_VMAX),

        /**
         * VMM.
         */
        MEDIATION_VMM(Component.MEDIATION_VMM),

        /**
         * VPlex.
         */
        MEDIATION_VPLEX(Component.MEDIATION_VPLEX),

        /**
         * Webhook.
         */
        MEDIATION_WEBHOOK(Component.MEDIATION_WEBHOOK),

        /**
         * XTremio.
         */
        MEDIATION_XTREMIO(Component.MEDIATION_XTREMIO),

        /**
         * Xen.
         */
        MEDIATION_XEN(Component.MEDIATION_XEN);


        private Component component;

        MediationComponent(@Nonnull final Component component) {
            this.component = component;
        }

        /**
         * Gets component.
         *
         * @return the component
         */
        @Nonnull
        public Component getComponent() {
            return component;
        }
    }
}
