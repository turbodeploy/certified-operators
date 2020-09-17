package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.history.HistoricalStatsServiceImpl;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;

/**
 * Spring configuration class for the HistoryService.
 */
@Configuration
@Import({HistoryClientConfig.class})
public class HistoryServiceConfig {
    /**
     * The history client configuration, which creates a history channel we need to connect to the history component.
     */
    @Autowired
    private HistoryClientConfig historyClientConfig;

    /**
     * Creates a new StatsHistoryServiceBlockingStub to use to communicate with the history component.
     *
     * @return a new StatsHistoryServiceBlockingStub
     */
    @Bean
    public StatsHistoryServiceBlockingStub historyRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

    /**
     * Creates the HistoricalStatsService, with the StatsHistoryServiceBlockingStub wired into it.
     *
     * @param historyRpcService The StatsHistoryServiceBlockingStub that the HistoricalStatsService will use to
     *                          communicate with the History component
     * @return The HistoricalStatsService
     */
    @Bean
    public HistoricalStatsService historicalStatsService(StatsHistoryServiceBlockingStub historyRpcService) {
        return new HistoricalStatsServiceImpl(historyRpcService);
    }
}
