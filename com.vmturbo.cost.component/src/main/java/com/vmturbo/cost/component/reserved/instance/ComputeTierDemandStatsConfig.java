package com.vmturbo.cost.component.reserved.instance;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.CostDBConfig;

/**
 * Configuration of computer tier demand stats.
 */
@Import({CostDBConfig.class})
public class ComputeTierDemandStatsConfig {

    @Autowired
    private CostDBConfig dbConfig;

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
        return new ComputeTierDemandStatsStore(
                dbConfig.dsl(),
                statsRecordsCommitBatchSize,
                statsRecordsQueryBatchSize);
    }

    @Bean
    public ComputeTierDemandStatsWriter riDemandStatsWriter() {
        return new ComputeTierDemandStatsWriter(
                riDemandStatsStore(),
                reservedInstanceConfig.projectedEntityRICoverageAndUtilStore(),
                preferredCurrentWeight);
    }
}
