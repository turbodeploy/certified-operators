package com.vmturbo.stitching.prestitching;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * This pre stitching operation is used to add connected network names of discovered vm based on
 * layeredOver entity UUIDs. We iterate all of the entities and group the networks by mapping UUID
 * to stitching entity. Then we iterate the VMs and match the grouped networks by layeredOver UUID
 * list, get the stitching entity and extract the display name of the network, and then add them
 * into VM {@link EntityDTO.VirtualMachineData.connectedNetwork} in response DTO.
 */
public class ConnectedNetworkPreStitchingOperation implements PreStitchingOperation {
    private static final Collection<EntityType> AFFECTED_ENTITY_TYPES =
                    ImmutableSet.of(EntityType.VIRTUAL_MACHINE, EntityType.NETWORK);

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
        @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.containsAllEntityTypesScope(
            ImmutableList.of(EntityType.VIRTUAL_MACHINE, EntityType.NETWORK));
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
        @Nonnull Stream<StitchingEntity> entities,
        @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // map for storing the <EntityType, UUID, stitching entity> triplets
        final Map<EntityType, Map<String, StitchingEntity>> entityTypeToUuidToEntity =
                        new EnumMap<>(EntityType.class);

        entities.forEach(stitchingEntity -> {
            final EntityType entityType = stitchingEntity.getEntityType();
            if (AFFECTED_ENTITY_TYPES.contains(entityType)) {
                final Map<String, StitchingEntity> uuidToEntity = entityTypeToUuidToEntity
                                .computeIfAbsent(entityType, k -> new HashMap<>());
                uuidToEntity.put(stitchingEntity.getEntityBuilder().getId(), stitchingEntity);
            }
        });

        final Map<String, StitchingEntity> uuidToVm = entityTypeToUuidToEntity
                        .getOrDefault(EntityType.VIRTUAL_MACHINE, Collections.emptyMap());
        final Map<String, StitchingEntity> uuidToNetwork = entityTypeToUuidToEntity
                        .getOrDefault(EntityType.NETWORK, Collections.emptyMap());
        /*
         go through each VM and update the connected network display name
         collect all non-network layered over uuids to retain them
         */
        for (StitchingEntity vm : uuidToVm.values()) {
            final EntityDTO.Builder vmBuilder = vm.getEntityBuilder();
             /*
              get all of the connected network display names of the VM and non-network
              layered over UUIDs to retain them after clearing layered over UUIDs.
              */
            final Map<String, String> connectedNetworkDisplayNames = new HashMap<>();
            final Collection<String> remainingUuids = new HashSet<>();
            vmBuilder.getLayeredOverList().forEach(layeredOverUuid -> {
                final StitchingEntity network = uuidToNetwork.get(layeredOverUuid);
                if (network != null) {
                    connectedNetworkDisplayNames.put(layeredOverUuid, network.getDisplayName());
                } else {
                    remainingUuids.add(layeredOverUuid);
                }
            });
            if (!connectedNetworkDisplayNames.isEmpty()) {
                resultBuilder.queueUpdateEntityAlone(vm, toBeUpdated -> {
                    final EntityDTO.Builder updatingBuilder = toBeUpdated.getEntityBuilder();
                    connectedNetworkDisplayNames.values()
                                    .forEach(connectedNetwork -> updatingBuilder
                                                    .getVirtualMachineDataBuilder()
                                                    .addConnectedNetwork(connectedNetwork));
                    clearAggregatedByConnection(toBeUpdated, StitchingEntity::getConnectedToByType,
                                    uuidToNetwork.keySet(),
                                    (updating, toRemove) -> updating.removeConnectedTo(toRemove,
                                                    ConnectionType.AGGREGATED_BY_CONNECTION));
                    updatingBuilder.clearLayeredOver();
                    updatingBuilder.addAllLayeredOver(remainingUuids);
                });
            }
            logger.debug("VM {} add connected network(s) {} and removed them from layered over relationships",
                            vm::getDisplayName, () -> connectedNetworkDisplayNames);
        }
        uuidToNetwork.values().forEach(network -> resultBuilder.queueUpdateEntityAlone(network,
                        toBeUpdated -> clearAggregatedByConnection(toBeUpdated,
                                        StitchingEntity::getConnectedFromByType, uuidToVm.keySet(),
                                        (updating, toRemove) -> updating
                                                        .removeConnectedFrom(toRemove,
                                                                        ConnectionType.AGGREGATED_BY_CONNECTION))));
        return resultBuilder.build();
    }

    private static void clearAggregatedByConnection(
                    StitchingEntity toBeUpdated,
                    Function<StitchingEntity, Map<ConnectionType, Set<StitchingEntity>>> connectedGetter,
                    Collection<String> uuids,
                    BiConsumer<StitchingEntity, StitchingEntity> removalFunction) {
        final Set<StitchingEntity> toRemove = connectedGetter.apply(toBeUpdated)
                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION,
                                        Collections.emptySet()).stream()
                        .filter(se -> uuids.contains(se.getEntityBuilder().getId()))
                        .collect(Collectors.toSet());
        toRemove.forEach(removing -> removalFunction.accept(toBeUpdated, removing));
    }

}
