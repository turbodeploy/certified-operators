package com.vmturbo.cost.component.rollup;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.db.DbAccessConfig;
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
     * {@link RollupTimesStore} for entity savings.
     *
     * @return The {@link RollupTimesStore} for entity savings.
     */
    @Bean
    public RollupTimesStore entitySavingsRollupTimesStore() {
        try {
            return new RollupTimesStore(dbAccessConfig.dsl(), RolledUpTable.ENTITY_SAVINGS);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create entitySavingsRollupTimesStore", e);
        }
    }

    /**
     * {@link RollupTimesStore} for billed costs.
     *
     * @return The {@link RollupTimesStore} for billed costs.
     */
    @Bean
    public RollupTimesStore billedCostRollupTimesStore() {
        try {
            return new RollupTimesStore(dbAccessConfig.dsl(), RolledUpTable.BILLED_COST);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create billedCostRollupTimesStore", e);
        }
    }
}
