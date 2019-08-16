package com.vmturbo.action.orchestrator.translation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

@Configuration
@Import({TopologyProcessorClientConfig.class})
public class ActionTranslationConfig {

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public ActionTranslator actionTranslator() {
        return new ActionTranslator(repositoryClientConfig.repositoryChannel());
    }
}
