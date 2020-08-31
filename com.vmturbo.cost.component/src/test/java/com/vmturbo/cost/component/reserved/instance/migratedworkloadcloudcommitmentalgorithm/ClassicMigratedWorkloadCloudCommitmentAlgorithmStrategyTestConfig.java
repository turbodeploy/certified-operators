package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.pricing.PriceTableStore;

/**
 * Spring configuration class used by the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest test class.
 */
@Profile("ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest")
@Configuration
public class ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTestConfig {
    /**
     * Create a mock implementation of the HistoricalStatsService.
     *
     * @return A mock implementation of the HistoricalStatsService
     */
    @Bean
    @Primary
    public HistoricalStatsService historicalStatsService() {
        return Mockito.mock(HistoricalStatsService.class);
    }

    /**
     * Creates a mock implementation of the PriceTableStore.
     * @return  A mock implementation of the PriceTableStore
     */
    @Bean
    @Primary
    public PriceTableStore priceTableStore() {
        return Mockito.mock(PriceTableStore.class);
    }


    /**
     * Creates a ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy with the historicalStatsService wired into it,
     * in the form of a MigratedWorkloadCloudCommitmentAlgorithmStrategy.
     *
     * @param historicalStatsService a ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy instance
     * @return MigratedWorkloadCloudCommitmentAlgorithmStrategy
     */
    @Bean
    @Primary
    public MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy(HistoricalStatsService historicalStatsService) {
        return new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(historicalStatsService,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
