package com.vmturbo.cost.component.rollup;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.sql.utils.ConditionalDbConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for database rollup beans.
 */
@Configuration
@Import({DbAccessConfig.class})
public class RollupConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    /**
     * {@link RollupTimesStore} for entity savings (bottom-up).
     *
     * @return The {@link RollupTimesStore} for entity savings.
     */
    @Bean
    public RollupTimesStore entitySavingsRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.ENTITY_SAVINGS);
    }

    /**
     * {@link RollupTimesStore} for entity savings (bill-based).
     *
     * @return The {@link RollupTimesStore} for entity savings.
     */
    @Bean
    public RollupTimesStore billedSavingsRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.BILLED_SAVINGS);
    }

    /**
     * {@link RollupTimesStore} for billed costs.
     *
     * @return The {@link RollupTimesStore} for billed costs.
     */
    @Bean
    public RollupTimesStore billedCostRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.BILLED_COST);
    }

    /**
     * Entity cost rollup time store.
     *
     * @return rollup time store
     */
    @Bean
    public RollupTimesStore entityCostRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.ENTITY_COST);
    }

    /**
     * Reserved Instance Coverage Time Store.
     *
     * @return the store
     */
    @Bean
    @Conditional(ConditionalDbConfig.DbEndpointCondition.class)
    public RollupTimesStore reservedInstanceCoverageRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.RESERVED_INSTANCE_COVERAGE);
    }

    /**
     * Reserved Instance Utilization Time Store.
     *
     * @return the store
     */
    @Bean
    @Conditional(ConditionalDbConfig.DbEndpointCondition.class)
    public RollupTimesStore reservedInstanceUtilizationRollupTimesStore() {
        return createRollupTimesStore(RolledUpTable.RESERVED_INSTANCE_UTILIZATION);
    }

    /**
     * Util to help create the rollup times store.
     *
     * @param rolledUpTable Table name enum.
     * @return RollupTimesStore instance.
     * @throws BeanCreationException Thrown on bean creation problem.
     */
    private RollupTimesStore createRollupTimesStore(RolledUpTable rolledUpTable)
            throws BeanCreationException {
        try {
            return new RollupTimesStore(dbAccessConfig.dsl(), rolledUpTable);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create " + rolledUpTable + " RollupTimesStore.", e);
        }
    }
}
