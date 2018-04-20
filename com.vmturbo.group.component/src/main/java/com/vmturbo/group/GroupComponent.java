package com.vmturbo.group;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import com.vmturbo.arangodb.ArangoHealthMonitor;
import com.vmturbo.common.protobuf.group.GroupDTOREST.DiscoveredGroupServiceController;
import com.vmturbo.common.protobuf.group.GroupDTOREST.GroupServiceController;
import com.vmturbo.common.protobuf.group.PolicyDTOREST.PolicyServiceController;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.group.api.server.GroupApiSecurityConfig;
import com.vmturbo.group.diagnostics.GroupDiagnosticsConfig;
import com.vmturbo.group.persistent.TemporaryGroupCache;
import com.vmturbo.group.service.DiscoveredGroupsRpcService;
import com.vmturbo.group.service.GroupService;
import com.vmturbo.group.service.PolicyService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration("theComponent")
@EnableDiscoveryClient
@Import({ArangoDBConfig.class,
        IdentityProviderConfig.class,
        GrpcConfig.class,
        SettingConfig.class,
        SQLDatabaseConfig.class,
        GroupApiSecurityConfig.class,
        GroupDiagnosticsConfig.class})
@EnableAutoConfiguration
public class GroupComponent extends BaseVmtComponent {

    @Autowired
    private ArangoDBConfig arangoDBConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GrpcConfig grpcConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private GroupDiagnosticsConfig diagnosticsConfig;

    private static Logger logger = LoggerFactory.getLogger(GroupComponent.class);

    @Value("${spring.application.name}")
    private String componentName;

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Value("${tempGroupExpirationTimeMins:10}")
    private int tempGroupExpirationTimeMins;

    @PostConstruct
    private void setup() {
        logger.info("Adding ArangoDB health check to the component health monitor.");
        // add a health monitor for Arango
        getHealthMonitor().addHealthCheck(
                new ArangoHealthMonitor(arangoHealthCheckIntervalSeconds, arangoDBConfig.arangoDriverFactory()::getDriver));

        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(
            new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,dbConfig.dataSource()::getConnection));
    }

    @Bean
    public PolicyService policyService() {
        return new PolicyService(arangoDBConfig.policyStore(),
            arangoDBConfig.groupStore(),
            identityProviderConfig.identityProvider());
    }

    @Bean
    public PolicyServiceController policyServiceController(final PolicyService policyService) {
        return new PolicyServiceController(policyService);
    }

    @Bean
    public GroupService groupService() {
        return new GroupService(arangoDBConfig.groupStore(),
                                arangoDBConfig.policyStore(),
                                temporaryGroupCache(),
                                grpcConfig.searchServiceBlockingStub());
    }

    @Bean
    public TemporaryGroupCache temporaryGroupCache() {
        return new TemporaryGroupCache(identityProviderConfig.identityProvider(), tempGroupExpirationTimeMins, TimeUnit.MINUTES);
    }


    @Bean
    public GroupServiceController groupServiceController(final GroupService groupService) {
        return new GroupServiceController(groupService);
    }

    @Bean
    public DiscoveredGroupsRpcService discoveredCollectionsRpcService() {
        return new DiscoveredGroupsRpcService(arangoDBConfig.groupStore(),
            arangoDBConfig.policyStore(), settingConfig.settingStore(), getHealthMonitor());
    }

    @Bean
    public DiscoveredGroupServiceController discoveredCollectionsServiceController() {
        return new DiscoveredGroupServiceController(discoveredCollectionsRpcService());
    }

    @Nonnull
    @Override
    protected Optional<Server> buildGrpcServer(@Nonnull ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(policyService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(groupService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(discoveredCollectionsRpcService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(settingConfig.settingService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(settingConfig.settingPolicyService(), monitoringInterceptor))
            .build());
    }

    public static void main(String[] args) {
        // apply the configuration properties for this component prior to Spring instantiation
        fetchConfigurationProperties();
        // instantiate and run this component
        new SpringApplicationBuilder()
                .sources(GroupComponent.class)
                .run(args);
    }

    @Override
    public String getComponentName() {
        return componentName;
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
    }
}
