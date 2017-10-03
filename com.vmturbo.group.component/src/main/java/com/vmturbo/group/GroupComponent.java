package com.vmturbo.group;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.arangodb.ArangoHealthMonitor;
import com.vmturbo.common.protobuf.group.GroupDTOREST.ClusterServiceController;
import com.vmturbo.common.protobuf.group.GroupDTOREST.DiscoveredCollectionsServiceController;
import com.vmturbo.common.protobuf.group.GroupDTOREST.GroupServiceController;
import com.vmturbo.common.protobuf.group.PolicyDTOREST.PolicyServiceController;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;
import com.vmturbo.group.service.ClusterRpcService;
import com.vmturbo.group.service.DiscoveredCollectionsRpcService;
import com.vmturbo.group.service.GroupService;
import com.vmturbo.group.service.PolicyService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration("theComponent")
@EnableDiscoveryClient
@Import({ArangoDBConfig.class,
    IdentityProviderConfig.class,
    GrpcConfig.class,
    SettingConfig.class,
    SQLDatabaseConfig.class})
@SpringBootApplication
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

    private static Logger logger = LoggerFactory.getLogger(GroupComponent.class);

    @Value("${spring.application.name}")
    private String componentName;

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        logger.info("Adding ArangoDB health check to the component health monitor.");
        // add a health monitor for Arango
        getHealthMonitor().addHealthCheck("ArangoDB",
                new ArangoHealthMonitor(arangoHealthCheckIntervalSeconds, arangoDBConfig.arangoDriverFactory()::getDriver));

        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
            new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,dbConfig.dataSource()::getConnection));
    }

    @Bean
    public PolicyService policyService() {
        return new PolicyService(arangoDBConfig.policyStore(),
            arangoDBConfig.groupStore(),
            arangoDBConfig.clusterStore(),
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
                                grpcConfig.searchServiceBlockingStub());
    }

    @Bean
    public GroupServiceController groupServiceController(final GroupService groupService) {
        return new GroupServiceController(groupService);
    }

    @Bean
    public ClusterRpcService clusterRpcService() {
        return new ClusterRpcService(arangoDBConfig.clusterStore());
    }

    @Bean
    public ClusterServiceController clusterServiceController() {
        return new ClusterServiceController(clusterRpcService());
    }

    @Bean
    public DiscoveredCollectionsRpcService discoveredCollectionsRpcService() {
        return new DiscoveredCollectionsRpcService(arangoDBConfig.groupStore(),
            arangoDBConfig.policyStore(), arangoDBConfig.clusterStore(), getHealthMonitor());
    }

    @Bean
    public DiscoveredCollectionsServiceController discoveredCollectionsServiceController() {
        return new DiscoveredCollectionsServiceController(discoveredCollectionsRpcService());
    }

    @Nonnull
    @Override
    protected Optional<Server> buildGrpcServer(@Nonnull ServerBuilder builder) {
        return Optional.of(builder
                .addService(policyService())
                .addService(groupService())
                .addService(clusterRpcService())
                .addService(discoveredCollectionsRpcService())
                .addService(settingConfig.settingService())
                .build());
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(GroupComponent.class)
                .run(args);
    }

    @Override
    public String getComponentName() {
        return componentName;
    }

}
