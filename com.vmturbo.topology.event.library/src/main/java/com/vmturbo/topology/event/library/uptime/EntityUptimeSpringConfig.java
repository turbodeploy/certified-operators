package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import com.vmturbo.cloud.common.data.BoundedDuration;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.common.protobuf.cost.EntityUptimeREST.EntityUptimeServiceController;
import com.vmturbo.topology.event.library.TopologyEventProvider;

/**
 * A spring configuration for entity uptime.
 */
@Configuration
public class EntityUptimeSpringConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private CloudScopeStore cloudScopeStore;

    @Autowired
    private TopologyEventProvider topologyEventProvider;

    @Value("${entityUptime.isEnabled:false}")
    boolean isUptimeEnabled;

    @Value("${entityUptime.uptimeFromCreation:true}")
    boolean uptimeFromCreation;

    @Value("${entityUptime.uptimeIntervalAmount:0}")
    private int uptimeIntervalAmount;

    @Value("${entityUptime.uptimeIntervalUnit:HOURS}")
    private String uptimeIntervalUnit;

    @Value("${entityUptime.cachedUptimeCalculation:false}")
    private boolean cachedUptimeCalculation;

    @Value("${entityUptime.uptimeWindowDuration:PT730H}")
    private String uptimeWindowDuration;

    /**
     * Creates a {@link EntityUptimeStore} instance.
     * @return The {@link EntityUptimeStore} instance.
     */
    @Bean
    @Nonnull
    public EntityUptimeStore entityUptimeStore() {
        return new InMemoryEntityUptimeStore(
                cloudScopeStore,
                isUptimeEnabled ? EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON : null);
    }

    /**
     * Creates a {@link EntityUptimeCalculator} instance.
     * @return The {@link EntityUptimeCalculator} instance.
     */
    @Bean
    @Nonnull
    public EntityUptimeCalculator entityUptimeCalculator() {
        return new EntityUptimeCalculator(uptimeCalculationRecorder(), uptimeFromCreation);
    }

    /**
     * Creates a {@link UptimeCalculationRecorder} instance.
     * @return The {@link UptimeCalculationRecorder} instance.
     */
    @Nonnull
    public UptimeCalculationRecorder uptimeCalculationRecorder() {
        return new UptimeCalculationRecorder();
    }

    /**
     * Creates a {@link EntityUptimeManager} instance.
     * @return The {@link EntityUptimeManager} instance.
     */
    @Bean
    @Nonnull
    public EntityUptimeManager entityUptimeManager() {
        return new EntityUptimeManager(
                topologyEventProvider,
                entityUptimeCalculator(),
                entityUptimeStore(),
                Duration.parse(uptimeWindowDuration),
                BoundedDuration.builder()
                        .amount(uptimeIntervalAmount)
                        .unit(ChronoUnit.valueOf(uptimeIntervalUnit))
                        .build(),
                cachedUptimeCalculation,
                isUptimeEnabled);
    }

    /**
     * Creates a {@link EntityUptimeRpcService} instance.
     * @return The {@link EntityUptimeRpcService} instance.
     */
    @Bean
    @Nonnull
    public EntityUptimeRpcService entityUptimeRpcService() {
        return new EntityUptimeRpcService(
                entityUptimeStore(),
                entityUptimeManager());
    }

    /**
     * Creates a {@link EntityUptimeServiceController} instance.
     * @return The {@link EntityUptimeServiceController} instance.
     */
    @Bean
    @Nonnull
    public EntityUptimeServiceController entityUptimeServiceController() {
        return new EntityUptimeServiceController(entityUptimeRpcService());
    }

    /**
     * Logs entity uptime settings and initializes the {@link #entityUptimeManager()}.
     */
    @EventListener(ContextRefreshedEvent.class)
    public void bootstrapInit() {

        logger.info("Entity uptime settings: {}",
                new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                        .append("uptimeFromCreation", uptimeFromCreation)
                        .append("uptimeIntervalAmount", uptimeIntervalAmount)
                        .append("uptimeIntervalUnit", uptimeIntervalUnit)
                        .append("cachedUptimeCalculation", cachedUptimeCalculation)
                        .append("uptimeWindowDuration", uptimeWindowDuration)
                        .build());

        logger.info("Running bootstrap initialization of entity uptime");
        entityUptimeManager().updateUptimeForTopology(true);
    }
}
