package com.vmturbo.cost.component.reserved.instance;


import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration of computer tier demand stats.
 */
@Import({DbAccessConfig.class})
@Configuration
public class ComputeTierDemandStatsConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Value("${statsRecordsCommitBatchSize:100}")
    private int statsRecordsCommitBatchSize;

    @Value("${statsRecordsQueryBatchSize:100}")
    private int statsRecordsQueryBatchSize;

    @Value("${preferredCurrentWeight:0.6}")
    private float preferredCurrentWeight;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Bean
    public ComputeTierDemandStatsStore riDemandStatsStore() {
        try {
            return new ComputeTierDemandStatsStore(dbAccessConfig.dsl(), statsRecordsCommitBatchSize,
                    statsRecordsQueryBatchSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ComputeTierDemandStatsStore bean", e);
        }
    }

    @Bean
    public ComputeTierDemandStatsWriter riDemandStatsWriter() {
        return new ComputeTierDemandStatsWriter(
                riDemandStatsStore(),
                reservedInstanceConfig.projectedEntityRICoverageAndUtilStore(),
                preferredCurrentWeight);
    }
}
