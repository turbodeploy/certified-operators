package com.vmturbo.repository.graph.result;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import org.immutables.value.Value;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Value.Immutable
public abstract class SupplyChainFluxResult {
    public abstract Mono<ServiceEntityRepoDTO> origin();
    public abstract Flux<TypeAndOids> providerResults();
    public abstract Flux<TypeAndOids> consumerResults();
}
