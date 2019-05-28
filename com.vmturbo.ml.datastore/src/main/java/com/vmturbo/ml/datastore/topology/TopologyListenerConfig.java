package com.vmturbo.ml.datastore.topology;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.ml.datastore.influx.InfluxConfig;
import com.vmturbo.ml.datastore.influx.Obfuscator;
import com.vmturbo.ml.datastore.influx.Obfuscator.HashingObfuscator;
import com.vmturbo.ml.datastore.rpc.MLDatastoreRpcConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Configuration for integration with the Topology Processor. The ML datstore receives
 * topology broadcasts from the TP and writes the metrics in those broadcasts to influx.
 */
@Configuration
@Import({
    TopologyProcessorClientConfig.class,
    MLDatastoreRpcConfig.class,
    InfluxConfig.class
})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    MLDatastoreRpcConfig rpcConfig;

    @Autowired
    private InfluxConfig influxConfig;


    @Bean
    public Obfuscator obfuscator() {
        return new HashingObfuscator();
    }

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        final TopologyEntitiesListener topologyEntitiesListener =
            new TopologyEntitiesListener(influxConfig.influxDBConnectionFactory(),
                influxConfig.metricsStoreWhitelist(), influxConfig.metricJitter(), obfuscator());
        topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        // Only listen to live topologies. Plan topology metrics are not stored because they
        // do not necessarily reflect what is really happening in the customer's environment.
        return tpConfig.topologyProcessor(TopologyProcessorSubscription.forTopic(Topic.LiveTopologies));
    }
}
