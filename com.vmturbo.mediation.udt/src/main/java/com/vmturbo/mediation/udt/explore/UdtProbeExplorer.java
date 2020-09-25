package com.vmturbo.mediation.udt.explore;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.mediation.udt.explore.collectors.AutomatedDefinitionCollector;
import com.vmturbo.mediation.udt.explore.collectors.ManualDefinitionCollector;
import com.vmturbo.mediation.udt.explore.collectors.UdtCollector;
import com.vmturbo.mediation.udt.inventory.UdtEntity;

/**
 * Class responsible for populating UDT entities {@link UdtEntity}.
 * - Request topology data definitions;
 * - Create collector for them: manual or automated.
 * - Execute collecting;
 */
public class UdtProbeExplorer {

    private static final Logger LOGGER = LogManager.getLogger();

    private final DataProvider dataProvider;
    private final ExecutorService executor;

    /**
     * Constructor.
     *
     * @param dataProvider -  a provider of models from Group and Repository components.
     * @param executor     -  executor service.
     */
    public UdtProbeExplorer(@Nonnull DataProvider dataProvider, @Nonnull ExecutorService executor) {
        this.dataProvider = dataProvider;
        this.executor = executor;
    }

    /**
     * Executes exploring of topology data definitions.
     *
     * @return set of {@link UdtEntity}.
     */
    public Set<UdtEntity> exploreDataDefinition() {
        final Map<Long, TopologyDataDefinition> definitionsMap = dataProvider.getTopologyDataDefinitions();
        final Set<UdtCollector> collectors = createUdtCollectors(definitionsMap);
        final Set<UdtEntity> definedEntities = collectEntities(collectors);
        return OidToUdtMappingTask.execute(definedEntities, dataProvider);
    }

    @Nonnull
    private Set<UdtCollector> createUdtCollectors(@Nonnull Map<Long, TopologyDataDefinition> definitionsMap) {
        return definitionsMap.entrySet().stream()
                .map(entry -> createCollector(entry.getKey(), entry.getValue()))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Nullable
    @VisibleForTesting
    @ParametersAreNonnullByDefault
    UdtCollector createCollector(Long definitionId, TopologyDataDefinition topologyDataDef) {
        switch (topologyDataDef.getTopologyDataDefinitionDetailsCase()) {
            case MANUAL_ENTITY_DEFINITION:
                return new ManualDefinitionCollector(definitionId, topologyDataDef.getManualEntityDefinition());
            case AUTOMATED_ENTITY_DEFINITION:
                return new AutomatedDefinitionCollector(definitionId, topologyDataDef.getAutomatedEntityDefinition());
            default:
                return null;
        }
    }

    @Nonnull
    private Set<UdtEntity> collectEntities(@Nonnull Set<UdtCollector> collectors) {
        final Collection<CompletableFuture<Set<UdtEntity>>> futures = collectors.stream()
                .map(this::createFutureTask)
                .collect(Collectors.toSet());
        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> futures.stream()
                        .map(this::getFutureCollectorResult)
                        .flatMap(Function.identity())
                        .collect(Collectors.toSet()))
                .join();
    }

    @Nonnull
    private CompletableFuture<Set<UdtEntity>> createFutureTask(@Nonnull UdtCollector<?> collector) {
        return CompletableFuture.supplyAsync(() -> collector.collectEntities(dataProvider), executor);
    }

    @Nonnull
    private Stream<UdtEntity> getFutureCollectorResult(@Nonnull CompletableFuture<Set<UdtEntity>> future) {
        try {
            return future.get().stream();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warn("Error while collecting entities: {}", e);
        }
        return Stream.empty();
    }
}
