package com.vmturbo.action.orchestrator.translation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

@Configuration
@Import({TopologyProcessorClientConfig.class, RepositoryClientConfig.class, GroupClientConfig.class})
public class ActionTranslationConfig {

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Bean
    public ActionTranslator actionTranslator() {
        return new ActionTranslator(repositoryClientConfig.repositoryChannel(),
            groupClientConfig.groupChannel());
    }
}
