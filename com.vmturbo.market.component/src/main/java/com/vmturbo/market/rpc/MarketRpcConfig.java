package com.vmturbo.market.rpc;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.market.InitialPlacementREST.InitialPlacementServiceController;
import com.vmturbo.common.protobuf.market.MarketDebugREST.MarketDebugServiceController;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;

import com.vmturbo.market.db.DbAccessConfig;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class, PlanOrchestratorClientConfig.class})
public class MarketRpcConfig {

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${prepareReservationCache:true}")
    private boolean prepareReservationCache;

    @Value("${maxRetry:1}")
    private int maxRetry;

    @Value("${maxGroupingRetry:5}")
    private int maxGroupingRetry;

    @Value("${maxRequestReservationTimeoutInSeconds:6000}")
    private long maxRequestReservationTimeoutInSeconds;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Bean
    public Optional<MarketDebugRpcService> marketDebugRpcService() {
        return grpcDebugServicesEnabled() ?
                Optional.of(new MarketDebugRpcService(realtimeTopologyContextId)) : Optional.empty();
    }

    @Bean
    public MarketDebugServiceController marketDebugServiceController() {
        return marketDebugRpcService().map(MarketDebugServiceController::new).orElse(null);
    }

    /**
     * Create the InitialPlacementRpcService. This service is used for reservation call from
     * plan orchestrator to market.
     *
     * @return A {@link InitialPlacementRpcService} instance.
     */
    @Bean
    public InitialPlacementRpcService initialPlacementRpcService() {
        return new InitialPlacementRpcService(getInitialPlacementFinder());
    }

    @Bean
    public InitialPlacementFinder getInitialPlacementFinder() {
        try {
            return new InitialPlacementFinder(dbAccessConfig.dsl(), getReservationService(),
                    prepareReservationCache, maxRetry, maxGroupingRetry);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create InitialPlacementFinder", e);
        }
    }

    @Bean
    public ReservationServiceBlockingStub getReservationService() {
        return ReservationServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }


    @Bean
    public ExecutorService getExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    /**
     * Fetch existing reservations from plan orchestrator once market component starts.
     */
    @Bean
    public void constructEconomyCachesFromDB() {
        getExecutorService().submit(() -> {
            getInitialPlacementFinder().restoreEconomyCaches(maxRequestReservationTimeoutInSeconds);
        });
    }

    /**
     * Create the InitialPlacementServiceController.
     *
     * @return A {@link InitialPlacementServiceController} instance.
     */
    @Bean
    public InitialPlacementServiceController initialPlacementServiceController() {
        return new InitialPlacementServiceController(initialPlacementRpcService());
    }

    private boolean grpcDebugServicesEnabled() {
        // This is a system property, not a consul property, because it's something we want to
        // hide completely in production deployments.
        return Optional.ofNullable(Boolean.getBoolean("grpc.debug.services.enabled"))
                .orElse(false);
    }
}
