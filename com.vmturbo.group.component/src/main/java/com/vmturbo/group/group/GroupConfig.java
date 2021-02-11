package com.vmturbo.group.group;

import java.util.concurrent.TimeUnit;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.flyway.V1_11_Callback;
import com.vmturbo.group.group.pagination.GroupPaginationConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

@Configuration
@Import({ActionOrchestratorClientConfig.class,
        IdentityProviderConfig.class,
        GroupComponentDBConfig.class,
        GroupPaginationConfig.class,
        TopologyProcessorClientConfig.class})
public class GroupConfig {

    @Value("${tempGroupExpirationTimeMins:30}")
    private int tempGroupExpirationTimeMins;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private GroupPaginationConfig groupPaginationConfig;

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    /**
     * Define flyway callbacks to be active during migrations for group component.
     *
     * @return array of callback objects
     */
    @Bean
    @Primary
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[] {
            new V1_11_Callback()
        };
    }

    @Bean
    public TemporaryGroupCache temporaryGroupCache() {
        return new TemporaryGroupCache(identityProviderConfig.identityProvider(),
                tempGroupExpirationTimeMins,
                TimeUnit.MINUTES);
    }

    @Bean
    public GroupDAO groupStore() {
        return new GroupDAO(databaseConfig.dsl(),
                groupPaginationConfig.groupPaginationParams());
    }

    /**
     * Cache for targets.
     *
     * @return the {@link ThinTargetCache}.
     */
    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(topologyProcessorClientConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.Notifications)));
    }

    /**
     * Calculates environment & cloud type for a group.
     *
     * @return the {@link GroupEnvironmentTypeResolver}.
     */
    @Bean
    public GroupEnvironmentTypeResolver groupEnvironmentTypeResolver() {
        return new GroupEnvironmentTypeResolver(thinTargetCache(), groupStore());
    }

    /**
     * Calculates severity for groups.
     *
     * @return the {@link GroupSeverityCalculator}.
     */
    @Bean
    public GroupSeverityCalculator groupSeverityCalculator() {
        return new GroupSeverityCalculator(aoClientConfig.entitySeverityClientCache());
    }
}
