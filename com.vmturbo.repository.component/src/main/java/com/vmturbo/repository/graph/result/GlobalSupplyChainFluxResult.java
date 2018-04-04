package com.vmturbo.repository.graph.result;

import org.immutables.value.Value;
import reactor.core.publisher.Flux;

@Value.Immutable
public abstract class GlobalSupplyChainFluxResult {
    public abstract Flux<SupplyChainOidsGroup> entities();
}
