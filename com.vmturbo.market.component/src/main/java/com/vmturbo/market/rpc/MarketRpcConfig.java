package com.vmturbo.market.rpc;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.market.MarketDebugREST.MarketDebugServiceController;

@Configuration
public class MarketRpcConfig {

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Bean
    public Optional<MarketDebugRpcService> marketDebugRpcService() {
        return grpcDebugServicesEnabled() ?
                Optional.of(new MarketDebugRpcService(realtimeTopologyContextId)) : Optional.empty();
    }

    @Bean
    public MarketDebugServiceController marketDebugServiceController() {
        return marketDebugRpcService().map(MarketDebugServiceController::new).orElse(null);
    }

    private boolean grpcDebugServicesEnabled() {
        // This is a system property, not a consul property, because it's something we want to
        // hide completely in production deployments.
        return Optional.ofNullable(Boolean.getBoolean("grpc.debug.services.enabled"))
                .orElse(false);
    }
}
