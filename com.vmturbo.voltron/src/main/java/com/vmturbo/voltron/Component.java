package com.vmturbo.voltron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import javax.annotation.Nonnull;
import javax.servlet.Servlet;

import com.google.common.collect.ImmutableMap;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jooq.Schema;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.action.orchestrator.ActionOrchestratorComponent;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.api.component.ApiComponent;
import com.vmturbo.auth.component.AuthComponent;
import com.vmturbo.auth.component.store.db.Auth;
import com.vmturbo.clustermgr.ClusterMgrMain;
import com.vmturbo.clustermgr.db.Clustermgr;
import com.vmturbo.cost.component.CostComponent;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.extractor.ExtractorComponent;
import com.vmturbo.extractor.schema.Extractor;
import com.vmturbo.group.GroupComponent;
import com.vmturbo.history.HistoryComponent;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.market.MarketComponent;
import com.vmturbo.market.component.db.Market;
import com.vmturbo.mediation.client.MediationComponentMain;
import com.vmturbo.mediation.webhook.component.WebhookComponent;
import com.vmturbo.plan.orchestrator.PlanOrchestratorComponent;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.repository.RepositoryComponent;
import com.vmturbo.repository.db.Repository;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.voltron.Voltron.VoltronContext;

/**
 * Enum to capture all the possible components that can be part of voltron.
 */
public enum Component {

    /**
     * The cluster manager.
     */
    CLUSTERMGR("clustermgr", "com.vmturbo.clustermgr",
            ClusterMgrMain.class,
            Optional.of(Clustermgr.CLUSTERMGR),
            ImmutableMap.of(
                    "clustermgr.consul.host", "localhost",
                    "clustermgr.consul.port", 8500,
                    // TODO: Once postgres DB feature flag is enabled, this will need to be fixed
                    "migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
                        "com.vmturbo.clustermgr/src/main/resources/db/migration"
                    )
            )
    ),

    /**
     * The auth component.
     */
    AUTH("auth", "com.vmturbo.auth.component", AuthComponent.class,
            Optional.of(Auth.AUTH),
            ImmutableMap.of(
                    "migrationLocation", "filesystem:" + Voltron.getAbsolutePath("com.vmturbo.auth.component/src/main/resources/db/migration")
            )),

    /**
     * The topology processor.
     */
    TOPOLOGY_PROCESSOR("topology-processor",
            "com.vmturbo.topology.processor", TopologyProcessorComponent.class,
            Optional.of(TopologyProcessor.TOPOLOGY_PROCESSOR),
            ImmutableMap.of(
                    "migrationLocation", "filesystem:" + Voltron.getAbsolutePath("com.vmturbo.topology.processor/src/main/resources/db/migration")
            )),

    /**
     * The market.
     */
    MARKET("market", "com.vmturbo.market.component", MarketComponent.class,
            Optional.of(Market.MARKET),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath("com.vmturbo.market.component/src/main/resources/db/migration"))),

    /**
     * The action orchestrator.
     */
    ACTION_ORCHESTRATOR("action-orchestrator",
            "com.vmturbo.action.orchestrator", ActionOrchestratorComponent.class,
            Optional.of(Action.ACTION),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
                    "com.vmturbo.action.orchestrator/src/main/resources/db/migration"))),

    /**
     * The history component.
     */
    HISTORY("history", "com.vmturbo.history", HistoryComponent.class,
            Optional.of(Vmtdb.VMTDB),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath("com.vmturbo.history.schema/src/main/resources/db/migration"))),

    /**
     * The plan orchestrator.
     */
    PLAN_ORCHESTRATOR("plan-orchestrator", "com.vmturbo.plan.orchestrator",
            PlanOrchestratorComponent.class, Optional.of(Plan.PLAN),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
                    "com.vmturbo.plan.orchestrator/src/main/resources/db/migration"))),

    /**
     * The cost component.
     */
    COST("cost", "com.vmturbo.cost.component", CostComponent.class,
            Optional.of(Cost.COST),
        ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
            "com.vmturbo.cost.component/src/main/resources/db/migration"))),

    /**
     * The group component.
     */
    GROUP("group", "com.vmturbo.group.component",
            GroupComponent.class,
            Optional.of(com.vmturbo.group.db.GroupComponent.GROUP_COMPONENT),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
            "com.vmturbo.group.component/src/main/resources/db/migration"))),

    /**
     * The repository component.
     */
    REPOSITORY("repository", "com.vmturbo.repository.component",
            RepositoryComponent.class, Optional.of(Repository.REPOSITORY),
            ImmutableMap.of("migrationLocation", "filesystem:" + Voltron.getAbsolutePath(
                    "com.vmturbo.repository.component/src/main/resources/db/migration"))),

    /**
     * The extractor component.
     */
    EXTRACTOR("extractor", "com.vmturbo.extractor",
            ExtractorComponent.class, Optional.of(Extractor.EXTRACTOR),
            ImmutableMap.<String, Object>builder()
                .put("dbMigrationLocation", Voltron.migrationLocation("com.vmturbo.extractor.schema"))
                .put("grafanaBuiltinDashboardPath", Voltron.getAbsolutePath("com.vmturbo.extractor/src/main/resources/dashboards"))
                .build()),

    /**
     * The API component.
     */
    API("api", "com.vmturbo.api.component", ApiComponent.class,
            Optional.empty(),
            ImmutableMap.<String, Object>builder().put("pom.name", "POMPOM")
                    // Temporary - API imports extractor schema.
                    .put("dbMigrationLocation", Voltron.migrationLocation("com.vmturbo.extractor.schema"))
                    .put("pom.version", "POMVERSION")
                    .put("turbo-version.commit.time", "never")
                    .put("timestamp", "future")
                    .build()),

    // START MEDIATION COMPONENTS ---

    /**
     * Actionscript mediation.
     */
    MEDIATION_ACTIONSCRIPT("actionscript", "com.vmturbo.mediation.actionscript.component"),

    /**
     * APM SNMP.
     */
    MEDIATION_APM_SNMP("apm-snmp", "com.vmturbo.mediation.apm.snmp.component"),

    /**
     * APM WMI.
     */
    MEDIATION_APM_WMI("apm-wmi", "com.vmturbo.mediation.apm.wmi.component"),

    /**
     * APPD.
     */
    MEDIATION_APPD("appd", "com.vmturbo.mediation.appdynamics.component"),

    /**
     * App insights.
     */
    MEDIATION_APPINSIGHTS("appinsights", "com.vmturbo.mediation.appinsights.component"),

    /**
     * AWS Billing.
     */
    MEDIATION_AWS_BILLING("aws-billing", "com.vmturbo.mediation.aws.billing.component"),

    /**
     * AWS.
     */
    MEDIATION_AWS("aws", "com.vmturbo.mediation.aws.component"),

    /**
     * AWS Cost.
     */
    MEDIATION_AWS_COST("aws-cost", "com.vmturbo.mediation.aws.cost.component"),

    /**
     * AWS Lambda.
     */
    MEDIATION_AWS_LAMBDA("aws-lambda", "com.vmturbo.mediation.aws.lambda.component"),

    /**
     * Azure.
     */
    MEDIATION_AZURE("azure", "com.vmturbo.mediation.azure.component"),

    /**
     * Azure cost.
     */
    MEDIATION_AZURE_COST("azure-cost", "com.vmturbo.mediation.azure.cost.component"),

    /**
     * Azure EA.
     */
    MEDIATION_AZURE_EA("azure-ea", "com.vmturbo.mediation.azure.ea.component"),

    /**
     * Azure SP.
     */
    MEDIATION_AZURE_SP("azure-sp", "com.vmturbo.mediation.azure.sp.component"),

    /**
     * Azure volumes.
     */
    MEDIATION_AZURE_VOLUMES("azure-volumes", "com.vmturbo.mediation.azure.volumes.component"),

    /**
     * Baremetal.
     */
    MEDIATION_BAREMETAL("baremetal", "com.vmturbo.mediation.baremetal.component"),

    /**
     * CloudFoundry.
     */
    MEDIATION_CLOUDFOUNDRY("cloudfoundry", "com.vmturbo.mediation.cloudfoundry.component"),

    /**
     * Compellent.
     */
    MEDIATION_COMPELLENT("compellent", "com.vmturbo.mediation.compellent.component"),

    /**
     * Custom data.
     */
    MEDIATION_CUSTOM_DATA("custom-data", "com.vmturbo.mediation.custom.data.component"),

    /**
     * DB MSSQL.
     */
    MEDIATION_DB_MSSQL("db-mssql", "com.vmturbo.mediation.database.mssql.component"),

    /**
     * DB MYSQL.
     */
    MEDIATION_DB_MYSQL("db-mysql", "com.vmturbo.mediation.database.mysql.component"),

    /**
     * Datadog.
     */
    MEDIATION_DATADOG("datadog", "com.vmturbo.mediation.datadog.component"),

    /**
     * Delegating probe.
     */
    MEDIATION_DELEGATING_PROBE("delegating-probe", "com.vmturbo.mediation.delegatingprobe.component"),

    /**
     * Dynatrace.
     */
    MEDIATION_DYNATRACE("dynatrace", "com.vmturbo.mediation.dynatrace.component"),

    /**
     * Flexera.
     */
    MEDIATION_FLEXERA("flexera", "com.vmturbo.mediation.flexera.component"),

    /**
     * GCP Service Account.
     */
    MEDIATION_GCP_SA("gcp-sa", "com.vmturbo.mediation.gcp.sa.component"),

    /**
     * GCP Project.
     */
    MEDIATION_GCP_PROJECT("gcp-project", "com.vmturbo.mediation.gcp.project.component"),

    /**
     * GCP Cost.
     */
    MEDIATION_GCP_COST("gcp-cost", "com.vmturbo.mediation.gcp.cost.component"),

    /**
     * GCP Billing.
     */
    MEDIATION_GCP_BILLING("gcp-billing", "com.vmturbo.mediation.gcp.billing.component"),

    /**
     * HDS.
     */
    MEDIATION_HDS("hds", "com.vmturbo.mediation.hds.component"),

    /**
     * Horizon.
     */
    MEDIATION_HORIZON("horizon", "com.vmturbo.mediation.horizon.component"),

    /**
     * Hpe3Par.
     */
    MEDIATION_HPE3PAR("hpe3par", "com.vmturbo.mediation.hpe3par.component"),

    /**
     * Hyperflex.
     */
    MEDIATION_HYPERFLEX("hyperflex", "com.vmturbo.mediation.hyperflex.component"),

    /**
     * HyperV.
     */
    MEDIATION_HYPERV("hyperv", "com.vmturbo.mediation.hyperv.component"),

    /**
     * Intersight - Hyperflex.
     */
    MEDIATION_INTERSIGHT_HYPERFLEX("intersight-hyperflex", "com.vmturbo.mediation.intersight.hyperflex.component"),

    /**
     * Intersight - Server.
     */
    MEDIATION_INTERSIGHT_SERVER("intersight-server", "com.vmturbo.mediation.intersight.server.component"),

    /**
     * Intersight - UCS.
     */
    MEDIATION_INTERSIGHT_UCS("intersight-ucs", "com.vmturbo.mediation.intersight.ucs.component"),

    /**
     * ISTIO.
     */
    MEDIATION_ISTIO("istio", "com.vmturbo.mediation.istio.component"),

    /**
     * ActionStream Kafka mediation.
     */
    MEDIATION_ACTIONSTREAM_KAFKA("mediation-actionstream-kafka", "com.vmturbo.mediation.actionstream.kafka.component"),

    /**
     * NetApp.
     */
    MEDIATION_NETAPP("netapp", "com.vmturbo.mediation.netapp.component"),

    /**
     * Netflow.
     */
    MEDIATION_NETFLOW("netflow", "com.vmturbo.mediation.netflow.component"),

    /**
     * NewRelic.
     */
    MEDIATION_NEWRELIC("newrelic", "com.vmturbo.mediation.newrelic.component"),

    /**
     * Nutanix.
     */
    MEDIATION_NUTANIX("nutanix", "com.vmturbo.mediation.nutanix.component"),

    /**
     * OneView.
     */
    MEDIATION_ONEVIEW("oneview", "com.vmturbo.mediation.oneview.component"),

    /**
     * Pivotal.
     */
    MEDIATION_PIVOTAL("pivotal", "com.vmturbo.mediation.pivotal.component"),

    /**
     * Pure.
     */
    MEDIATION_PURE("pure", "com.vmturbo.mediation.pure.component"),

    /**
     * RHV.
     */
    MEDIATION_RHV("rhv", "com.vmturbo.mediation.rhv.component"),

    /**
     * ScaleIO.
     */
    MEDIATION_SCALEIO("scaleio", "com.vmturbo.mediation.scaleio.component"),

    /**
     * Storage stress probe.
     */
    MEDIATION_STORAGE_STRESS("storage-stress", "com.vmturbo.mediation.storagestressprobe.component"),

    /**
     * Stress probe.
     */
    MEDIATION_STRESS("stress", "com.vmturbo.mediation.stressprobe.component"),

    /**
     * Terraform.
     */
    MEDIATION_TERRAFORM("terraform", "com.vmturbo.mediation.terraform.component"),

    /**
     * Tetration.
     */
    MEDIATION_TETRATION("tetration", "com.vmturbo.mediation.tetration.component"),

    /**
     * Tomcat.
     */
    MEDIATION_TOMCAT("tomcat", "com.vmturbo.mediation.tomcat.component"),

    /**
     * UCS.
     */
    MEDIATION_UCS("ucs", "com.vmturbo.mediation.ucs.component"),

    /**
     * UCS Director.
     */
    MEDIATION_UCS_DIRECTOR("ucs-director", "com.vmturbo.mediation.ucsdirector.component"),

    /**
     * Util probe.
     */
    MEDIATION_UTIL_PROBE("util-probe", "com.vmturbo.mediation.utilprobe.component"),

    /**
     * VCD.
     */
    MEDIATION_VCD("vcd", "com.vmturbo.mediation.vcd.component"),

    /**
     * VC Browsing.
     */
    MEDIATION_VC_BROWSING("vc-browsing", "com.vmturbo.mediation.vcenter.browsing.component"),

    /**
     * VC.
     */
    MEDIATION_VC("vc", "com.vmturbo.mediation.vcenter.component"),

    /**
     * VMax.
     */
    MEDIATION_VMAX("vmax", "com.vmturbo.mediation.vmax.component"),

    /**
     * VMM.
     */
    MEDIATION_VMM("vmm", "com.vmturbo.mediation.vmm.component"),

    /**
     * VPlex.
     */
    MEDIATION_VPLEX("vplex", "com.vmturbo.mediation.vplex.component"),

    /**
     * Webhook.
     */
    MEDIATION_WEBHOOK("webhook", "com.vmturbo.mediation.webhook.component", WebhookComponent.class),

    /**
     * XTremio.
     */
    MEDIATION_XTREMIO("xtremio", "com.vmturbo.mediation.xtremio.component"),

    /**
     * ServiceNOW.
     */
    MEDIATION_SERVICENOW("servicenow", "com.vmturbo.mediation.servicenow.component"),

    /**
     * Xen.
     */
    MEDIATION_XEN("xen", "com.vmturbo.mediation.xen.component");

    private final String shortName;
    private final String topLevelFolder;
    private final Class<?> configClass;
    private final Map<String, Object> customProps;
    private final Optional<Schema> componentDbSchema;

    Component(
            final String probeShortName,
            final String probeTopFolder,
            Class<?> configClass
    ) {
        this(probeShortName, probeTopFolder,
                configClass,
                Optional.empty(),
                ImmutableMap.of("probe-directory",
                        Voltron.getAbsolutePath(probeTopFolder + "/target/probe-jars")));
    }

    Component(
            final String probeShortName,
            final String probeTopFolder
    ) {
        this(probeShortName, probeTopFolder,
            MediationComponentMain.class);
    }

    Component(
            final String shortName,
            final String topLevelFolder,
            Class<?> configClass,
            Optional<Schema> componentDbSchema,
            final Map<String, Object> extraProps
    ) {
        this.shortName = shortName;
        this.topLevelFolder = topLevelFolder;
        this.customProps = extraProps;
        this.componentDbSchema = componentDbSchema;
        this.configClass = configClass;
    }

    Component(
            final String shortName,
            final String topLevelFolder,
            Class<?> configClass,
            Optional<Schema> componentDbSchema
    ) {
        this(shortName, topLevelFolder, configClass, componentDbSchema, Collections
                .emptyMap());
    }

    /**
     * Add the component to the voltron Spring context.
     *
     * @param contextServer The context handler (to add the servlet to).
     * @param propertyRegistry Property registry tolook up property values.
     * @param voltronContext The {@link VoltronContext} for the voltron under construction.
     * @return The {@link ServletHolder}.
     * @throws IOException If there is an issue.
     */
    @Nonnull
    public ServletHolder addToContext(@Nonnull final ServletContextHandler contextServer,
                                      final PropertyRegistry propertyRegistry,
                                      VoltronContext voltronContext) throws IOException {
        final List<PropertySource<?>> props = new ArrayList<>();
        // Custom properties added first, because they have top priority.
        if (!customProps.isEmpty()) {
            props.add(new MapPropertySource("custom", customProps));
        }
        // Component specific properties have second priority.
        props.add(propertyRegistry.getComponentProperties(shortName, topLevelFolder,
                getPathPrefix()));

        // See if this component has a properties file.
        ConfigurableEnvironment env = Voltron.createEnvironment(props);

        final AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        context.register(configClass);
        context.setNamespace(topLevelFolder);
        context.setEnvironment(env);
        if (this == API) {
            // Stupid hack. We want to (and need to) serve the API from the root context,
            // so we add the dispatcher servlet here.
            final ServletHolder dispatcherServlet = ApiComponent.addDispatcherToContext(context, contextServer);
            // Make it initialize after the parent context.
            dispatcherServlet.setInitOrder(100);
        }

        voltronContext.addChildContext(this, context);

        return addServlet(context, contextServer);
    }

    /**
     * Get the API path prefix to serve the component's API at.
     * Each component is in a servlet, and this is the URL prefix to access the servlet.
     *
     * @return The path prefix.
     */
    @Nonnull
    public String getPathPrefix() {
        final StringJoiner pathJoiner = new StringJoiner("/", "/", "");
        if (this == API) {
            // /api is taken by the actual external API.
            pathJoiner.add("api_component");
        } else {
            pathJoiner.add(shortName);
        }
        return pathJoiner.toString();
    }

    private ServletHolder addServlet(final AnnotationConfigWebApplicationContext context, @Nonnull ServletContextHandler contextServer) {
        String path = getPathPrefix() + "/";
        final Servlet servlet = new DispatcherServlet(context);
        final ServletHolder servletHolder = new ServletHolder(servlet);
        servletHolder.setInitOrder(this.ordinal());
        ((DispatcherServlet)servlet).setNamespace(shortName);
        contextServer.addServlet(servletHolder, path + "*");
        contextServer.addServlet(servletHolder, path);
        return servletHolder;
    }

    /**
     * Look up a component by name.
     *
     * @param shortName The name ({@link Component#getShortName()}.
     * @return The {@link Component}, if found.
     */
    public static Optional<Component> forShortName(String shortName) {
        for (Component component : Component.values()) {
            if (shortName.equalsIgnoreCase(component.shortName)) {
                return Optional.of(component);
            }
        }
        return Optional.empty();
    }

    public String getShortName() {
        return shortName;
    }

    @Nonnull
    public Optional<Schema> getDbSchema() {
        return componentDbSchema;
    }
}
