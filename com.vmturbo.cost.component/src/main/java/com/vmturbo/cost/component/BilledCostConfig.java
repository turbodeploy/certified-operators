package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.BatchInserter;
import com.vmturbo.cost.component.billedcosts.BilledCostStore;
import com.vmturbo.cost.component.billedcosts.BilledCostUploadRpcService;
import com.vmturbo.cost.component.billedcosts.RollupBilledCostProcessor;
import com.vmturbo.cost.component.billedcosts.SqlBilledCostStore;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagIdentityService;
import com.vmturbo.cost.component.billedcosts.TagStore;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.rollup.RollupConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for BilledCostUploadRpcService.
 */
@Configuration
@Import({IdentityProviderConfig.class, DbAccessConfig.class})
public class BilledCostConfig {
    /**
     * How long in minutes after 12:00AM to run the daily processor task.
     */
    private static final int PROCESSOR_MINUTE_MARK = 90;

    private static final long DAY_MINUTES = TimeUnit.DAYS.toMinutes(1);

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Autowired
    private RollupConfig rollupConfig;

    @Autowired
    private TimeFrameCalculator timeFrameCalculator;

    @Value("${tagPersistence.batchSize:1000}")
    private int tagPersistenceBatchSize;

    @Value("${billedCostDataBatchSize:800}")
    private int billedCostDataBatchSize;

    @Value("${parallelBatchInserts:5}")
    private int parallelBatchInserts;

    /**
     * Returns an instance of BilledCostUploadRpcService.
     *
     * @return an instance of BilledCostUploadRpcService.
     */
    @Bean
    public BilledCostUploadRpcService billedCostUploadRpcService() {
        return new BilledCostUploadRpcService(tagGroupIdentityService(), billedCostStore());
    }

    /**
     * Returns an instance of BilledCostStoreFactory.
     *
     * @return an instance of BilledCostStoreFactory.
     */
    @Bean
    public BilledCostStore billedCostStore() {
        try {
            return new SqlBilledCostStore(dbAccessConfig.dsl(), batchInserter(),
                timeFrameCalculator);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create billedCostStore", e);
        }
    }

    /**
     * Returns an instance of TagGroupIdentityResolver.
     *
     * @return an instance of TagGroupIdentityResolver.
     */
    @Bean
    public TagGroupIdentityService tagGroupIdentityService() {
        return new TagGroupIdentityService(tagGroupStore(), tagIdentityService(), identityProvider(),
                billedCostDataBatchSize);
    }

    /**
     * Returns an instance of TagIdentityResolver.
     *
     * @return an instance of TagIdentityResolver.
     */
    @Bean
    public TagIdentityService tagIdentityService() {
        return new TagIdentityService(tagStore(), identityProvider(), billedCostDataBatchSize);
    }

    /**
     * Returns an instance of TagGroupStore.
     *
     * @return an instance of TagGroupStore.
     */
    @Bean
    public TagGroupStore tagGroupStore() {
        try {
            return new TagGroupStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create tagGroupStore", e);
        }
    }

    /**
     * Returns an instance of TagStore.
     *
     * @return an instance of TagStore.
     */
    @Bean
    public TagStore tagStore() {
        try {
            return new TagStore(dbAccessConfig.dsl(), tagPersistenceBatchSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create tagGroupStore", e);
        }
    }

    /**
     * Returns an instance of BatchInserter.
     *
     * @return an instance of BatchInserter.
     */
    @Bean
    public BatchInserter batchInserter() {
        logger.info("Initializing batchInserter, billedCostDataBatchSize: {}, parallelBatchInserts: {}",
                billedCostDataBatchSize, parallelBatchInserts);
        return new BatchInserter(billedCostDataBatchSize, parallelBatchInserts,
                rollupConfig.billedCostRollupTimesStore());
    }

    /**
     * Returns an instance of IdentityProvider.
     *
     * @return an instance of IdentityProvider.
     */
    public IdentityProvider identityProvider() {
        return identityProviderConfig.identityProvider();
    }

    /**
     * Task that executes once every day to process billed cost rollup and retention.
     *
     * @return singleton instance of {@code RollupBilledCostProcessor}.
     */
    @Bean
    public RollupBilledCostProcessor rollupBilledCostProcessor() {
        final RollupBilledCostProcessor rollupBilledCostProcessor = new RollupBilledCostProcessor(
                billedCostStore(),
                rollupConfig.billedCostRollupTimesStore(),
                costComponentGlobalConfig.clock());
        final LocalDateTime now = LocalDateTime.now();
        LocalDateTime firstRunTime = LocalDate.now().atStartOfDay().plusMinutes(PROCESSOR_MINUTE_MARK);
        if (firstRunTime.isBefore(now)) {
            firstRunTime = firstRunTime.plusDays(1);
        }
        final long initialDelayMinutes = ChronoUnit.MINUTES.between(now, firstRunTime);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                rollupBilledCostProcessor::process, initialDelayMinutes, DAY_MINUTES, TimeUnit.MINUTES);

        logger.info("BilledCostProcessor is enabled, will run at 12AM+{} min, after {} mins.",
                PROCESSOR_MINUTE_MARK, initialDelayMinutes);

        return rollupBilledCostProcessor;
    }
}
