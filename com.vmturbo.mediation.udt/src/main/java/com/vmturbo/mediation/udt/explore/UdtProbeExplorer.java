package com.vmturbo.mediation.udt.explore;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.annotations.VisibleForTesting;

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

    private final DataProvider dataProvider;

    /**
     * Constructor.
     *
     * @param dataProvider -  a provider of models from Group and Repository components.
     */
    public UdtProbeExplorer(@Nonnull DataProvider dataProvider) {
        this.dataProvider = dataProvider;
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
        return collectors.stream()
                .map(collector -> (Set<UdtEntity>)collector.collectEntities(dataProvider))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }
}
