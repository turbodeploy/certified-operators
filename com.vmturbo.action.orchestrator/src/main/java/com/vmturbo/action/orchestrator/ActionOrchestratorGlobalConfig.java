package com.vmturbo.action.orchestrator;

import java.time.Clock;
import java.util.EnumSet;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Global beans for the component that don't belong in any
 * specific package.
 */
@Configuration
@Import({TopologyProcessorClientConfig.class})
public class ActionOrchestratorGlobalConfig {

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpClientConfig.topologyProcessor(TopologyProcessorSubscription.forTopic(Topic.Notifications));
    }

    @Bean
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    /**
     * GSON HTTP converter configured to support swagger.
     * (see: http://stackoverflow.com/questions/30219946/springfoxswagger2-does-not-work-with-gsonhttpmessageconverterconfig/30220562#30220562)
     *
     * @return The {@link GsonHttpMessageConverter}.
     */
    @Bean
    public GsonHttpMessageConverter gsonHttpMessageConverter() {
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        return msgConverter;
    }

    @Bean
    public Channel topologyProcessorChannel() {
        return tpClientConfig.topologyProcessorChannel();
    }

    public Channel repositoryProcessorChannel() {
        return repositoryClientConfig.repositoryChannel();
    }

    @Bean
    public Clock actionOrchestratorClock() {
        return Clock.systemUTC();
    }
}

