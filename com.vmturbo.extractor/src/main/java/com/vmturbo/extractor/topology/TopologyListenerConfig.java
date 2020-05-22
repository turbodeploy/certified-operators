package com.vmturbo.extractor.topology;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({
        TopologyProcessorClientConfig.class,
        GroupClientConfig.class,
        ExtractorDbConfig.class
})
public class TopologyListenerConfig {
    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;


    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * How often we update last-seen timestamps for entities whose hash values have not changed.
     *
     * <p>The last-seen timestamps need not be precise, so to optimize ingestion performance we
     * only update the values occasionally.</p>
     */
    @Value("${lastSeenUpdateIntervalMinutes:360}")
    private int lastSeenUpdateIntervalMinutes;

    /**
     * Fuzz factor added to last-seen timestamp estimates to provide added tolerance for delayed
     * ingestions.
     */
    @Value("${lastSeenAdditionalFuzzMinutes:60}")
    private int lastSeenAdditionalFuzzMinutes;

    /**
     * Max time to wait for results of COPY FROM command that streams data to postgres, after
     * all records have been sent.
     */
    @Value("${insertTimeoutSeconds:300}")
    private int insertTimeoutSeconds;

    /**
     * Create an instance of our topology listener.
     *
     * @return listener instance
     * @throws UnsupportedDialectException if the associated db endpoint is mis-configured
     */
    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() throws UnsupportedDialectException {
        final ImmutableList<Supplier<? extends ITopologyWriter>> writerFactories =
                ImmutableList.<Supplier<? extends ITopologyWriter>>builder()
                        .addAll(writerFactories())
                        .build();
        final TopologyEntitiesListener topologyEntitiesListener = new TopologyEntitiesListener(
                groupServiceBlockingStub(), writerFactories, writerConfig());
        topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }

    /**
     * Create a {@link WriterConfig} object encapsulating configuration properties for the writer.
     *
     * @return writer config
     */
    @Bean
    public WriterConfig writerConfig() {
        return ImmutableWriterConfig.builder()
                .lastSeenUpdateIntervalMinutes(lastSeenUpdateIntervalMinutes)
                .lastSeenAdditionalFuzzMinutes(lastSeenAdditionalFuzzMinutes)
                .insertTimeoutSeconds(insertTimeoutSeconds)
                .build();

    }

    /**
     * Create a topology processor subscribing to realtime source topologies.
     *
     * @return topology processor
     */
    @Bean
    public TopologyProcessor topologyProcessor() {
        // Only listen to REALTIME SOURCE topologies.
        // TODO: Also listen for live topology summaries
        return tpConfig.topologyProcessor(TopologyProcessorSubscription.forTopic(Topic.LiveTopologies));
    }

    /**
     * Create a group service endpoint.
     *
     * @return service endpoint
     */
    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Create list of factories for writers that will participate in topology processing.
     *
     * @return writer factories
     * @throws UnsupportedDialectException if there's a problem getting the associated db endpoint
     */
    @Bean
    public List<Supplier<ITopologyWriter>> writerFactories()
            throws UnsupportedDialectException {
        final DbEndpoint dbEndpoint = extractorDbConfig.ingesterEndpoint();
        return ImmutableList.of(
                () -> new EntityMetricWriter(dbEndpoint, pool())
        );
    }

    /**
     * Create a thread pool for use by the writers.
     *
     * @return thread pool
     */
    @Bean
    public ExecutorService pool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("extractor-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}

