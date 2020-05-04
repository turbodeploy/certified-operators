package com.vmturbo.action.orchestrator;

import java.time.Clock;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Global beans for the component that don't belong in any
 * specific package.
 */
@Configuration
@Import({TopologyProcessorClientConfig.class})
public class ActionOrchestratorGlobalConfig {

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

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

    public Channel repositoryProcessorChannel() {
        return repositoryClientConfig.repositoryChannel();
    }

    @Bean
    public Clock actionOrchestratorClock() {
        return Clock.systemUTC();
    }
}

