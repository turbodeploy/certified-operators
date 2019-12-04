package com.vmturbo.cost.component.reserved.instance;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Import({SQLDatabaseConfig.class})
public class ComputeTierDemandStatsConfig {

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Value("${statsRecordsCommitBatchSize}")
    private int statsRecordsCommitBatchSize;

    @Value("${statsRecordsQueryBatchSize}")
    private int statsRecordsQueryBatchSize;

    @Value("${preferredCurrentWeight}")
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
