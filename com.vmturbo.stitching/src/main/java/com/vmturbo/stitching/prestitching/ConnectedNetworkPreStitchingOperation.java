package com.vmturbo.stitching.prestitching;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import jersey.repackaged.com.google.common.collect.Maps;

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
        // list for storing all virtual machines
        List<StitchingEntity> vms = Lists.newArrayList();
        // map for storing the <UUID, network display name> pairs
        Map<String, String> uuidToNetworks = Maps.newHashMap();

        entities.forEach(stitchingEntity -> {
            EntityType entityType = stitchingEntity.getEntityType();
            EntityDTO.Builder builder = stitchingEntity.getEntityBuilder();
            if (entityType == EntityType.VIRTUAL_MACHINE) {
                vms.add(stitchingEntity);
            } else if (entityType == EntityType.NETWORK) {
                uuidToNetworks.put(builder.getId(), builder.getDisplayName());
            }
        });

        // go through each VM and update the connected network display name
        for (StitchingEntity vm : vms) {
            EntityDTO.Builder vmBuilder = vm.getEntityBuilder();
            // get all of the connected network display names of the VM
            List<String> connectedNetworks = vmBuilder.getLayeredOverList().stream()
                .filter(uuidToNetworks::containsKey)
                .map(uuidToNetworks::get)
                .collect(Collectors.toList());

            if (!connectedNetworks.isEmpty()) {
                resultBuilder.queueUpdateEntityAlone(vm, toBeUpdated ->
                    addConnectedNetworks(toBeUpdated, connectedNetworks));
            }

            logger.debug("VM {} add connected network(s) {}.", vm.getDisplayName(),
                    connectedNetworks);
        }

        return resultBuilder.build();
    }

    /**
     * Add connected networks for vm by adding all of the network display names to the VM
     * {@link EntityDTO.VirtualMachineData.connectedNetwork}
     *
     * @param vm the VM whose connected networks are to be added
     * @param connectedNetworks list of all of connected network display names
     */
    private void addConnectedNetworks(@Nonnull StitchingEntity vm,
        @Nonnull List<String> connectedNetworks) {
        connectedNetworks.stream().forEach(connectedNetwork -> vm.getEntityBuilder()
            .getVirtualMachineDataBuilder().addConnectedNetwork(connectedNetwork));
    }
}
