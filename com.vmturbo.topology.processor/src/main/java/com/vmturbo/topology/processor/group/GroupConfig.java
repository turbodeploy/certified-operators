package com.vmturbo.topology.processor.group;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupFetcher;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.PolicyFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;

/**
 * The configuration for dealing with groups.
 */
@Configuration
@Import({GlobalConfig.class, EntityConfig.class})
public class GroupConfig {

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Value("${groupHost}")
    private String groupHost;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${groupFetcherTimeoutSeconds}")
    private long groupFetcherTimeoutSeconds;

    @Value("${discoveredGroupUploadIntervalSeconds}")
    private long discoveredGroupUploadIntervalSeconds;

    @Bean
    public Channel groupChannel() {
        return PingingChannelBuilder.forAddress(groupHost, globalConfig.grpcPort())
            .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
            .usePlaintext(true)
            .build();
    }

    @Bean
    public PolicyServiceBlockingStub policyRpcService() {
        return PolicyServiceGrpc.newBlockingStub(groupChannel());
    }

    @Bean
    public GroupFetcher groupFetcher() {
        return new GroupFetcher(groupChannel(), Duration.ofSeconds(groupFetcherTimeoutSeconds));
    }

    @Bean
    public TopologyFilterFactory topologyFilterFactory() {
        return new TopologyFilterFactory();
    }

    @Bean
    public PolicyManager policyManager() {
        return new PolicyManager(policyRpcService(), groupFetcher(),
                topologyFilterFactory(), new PolicyFactory());
    }

    @Bean
    public DiscoveredGroupUploader discoveredGroupUploader() {
        return new DiscoveredGroupUploader(groupChannel(), entityConfig.entityStore());
    }
}
