package com.vmturbo.stitching.prestitching;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        // map for storing the <EntityType, target oid, UUID, stitching entity>
        final Map<Long, Map<String, StitchingEntity>> targetToUuidToVms = new HashMap<>();
        final Map<Long, Map<String, StitchingEntity>> targetToUuidToNetworks = new HashMap<>();
        entities.forEach(stitchingEntity -> {
            final EntityType entityType = stitchingEntity.getEntityType();
            Map<Long, Map<String, StitchingEntity>> targetOidToUuidToEntity = null;
            if (entityType == EntityType.VIRTUAL_MACHINE) {
                targetOidToUuidToEntity = targetToUuidToVms;
            }
            if (entityType == EntityType.NETWORK) {
                targetOidToUuidToEntity = targetToUuidToNetworks;
            }
            /*
             Here collection of entities is used as a value, because local UUID is used as a
             key, which has no any unique guarantee across several targets. Here entities from
             all targets are going to be processed.
             */
            if (targetOidToUuidToEntity != null) {
                targetOidToUuidToEntity.computeIfAbsent(stitchingEntity.getTargetId(),
                                k -> new HashMap<>())
                                .put(stitchingEntity.getEntityBuilder().getId(), stitchingEntity);
            }
        });

        /*
         go through each VM and update the connected network display name
         collect all non-network layered over uuids to retain them
         */
        for (Map.Entry<Long, Map<String, StitchingEntity>> targetToLocalIdToVms : targetToUuidToVms
                        .entrySet()) {
            final Long targetId = targetToLocalIdToVms.getKey();
            final Map<String, StitchingEntity> uuidToNetworks =
                            targetToUuidToNetworks.getOrDefault(targetId, Collections.emptyMap());
            if (uuidToNetworks.isEmpty()) {
                continue;
            }
            for (StitchingEntity vm : targetToLocalIdToVms.getValue().values()) {
                final EntityDTO.Builder vmBuilder = vm.getEntityBuilder();
                    /*
                     get all of the connected network display names of the VM and non-network
                     layered over UUIDs to retain them after clearing layered over UUIDs.
                     */
                final Map<String, String> connectedNetworkDisplayNames =
                                new HashMap<>();
                final Collection<String> remainingUuids = new HashSet<>();
                vmBuilder.getLayeredOverList().forEach(layeredOverUuid -> {
                    final StitchingEntity network =
                                    uuidToNetworks.get(layeredOverUuid);
                    if (network == null) {
                        remainingUuids.add(layeredOverUuid);
                    } else {
                        connectedNetworkDisplayNames
                                        .putIfAbsent(layeredOverUuid, network.getDisplayName());
                    }
                });
                if (!connectedNetworkDisplayNames.isEmpty()) {
                    resultBuilder.queueUpdateEntityAlone(vm, toBeUpdated -> {
                        final EntityDTO.Builder updatingBuilder = toBeUpdated.getEntityBuilder();
                        connectedNetworkDisplayNames.values().forEach(networkDisplayName -> {
                            updatingBuilder.getVirtualMachineDataBuilder()
                                            .addConnectedNetwork(networkDisplayName);
                        });
                        updatingBuilder.clearLayeredOver();
                        updatingBuilder.addAllLayeredOver(remainingUuids);
                    });
                }
                logger.debug("VM {} add connected network(s) {} and removed them from layered over relationships",
                                () -> vm, () -> connectedNetworkDisplayNames);
            }
        }
        return resultBuilder.build();
    }
}
