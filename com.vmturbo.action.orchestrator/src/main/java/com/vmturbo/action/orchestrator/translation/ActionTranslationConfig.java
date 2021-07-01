package com.vmturbo.action.orchestrator.translation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.group.api.GroupClientConfig;

@Configuration
@Import({GroupClientConfig.class, TopologyProcessorConfig.class})
public class ActionTranslationConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Bean
    public ActionTranslator actionTranslator() {
        return new ActionTranslator(groupClientConfig.groupChannel(), tpConfig.actionTopologyStore());
    }
}
