package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;

/**
 * This stage checks if there're Fabric targets in entityStore, and if there're
 * then makes a linear search entityStore, gets PMs and saves info about their
 * connections to datacenters in {@link DiscoveredGroupUploader}.
 */
public class DCMappingStage extends Stage<EntityStore, EntityStore> {
    private final DiscoveredGroupUploader discoveredGroupUploader;

    /**
     * Constructor for the stage.
     * @param groupUploader a {@link DiscoveredGroupUploader} instance that will be
     *      keeping the desired data.
     */
    public DCMappingStage(final DiscoveredGroupUploader groupUploader) {
        this.discoveredGroupUploader = groupUploader;
    }

    @NotNull
    @Override
    public StageResult<EntityStore> execute(@NotNull EntityStore input)
                    throws PipelineStageException, InterruptedException {
        if (!discoveredGroupUploader.isFabricTargetPresent())   {
            return StageResult.withResult(input)
                .andStatus(Status.success("No Fabric targets. Skipping DCMappingStage."));
        }

        StringBuilder status = new StringBuilder("Fabric targets are present.");
        // A map of SDK String IDs to XL OIDs:
        Map<String, Long> datacenterIDsToOIDs = new HashMap<>();
        List<Entity> hosts = new ArrayList<>();
        input.getAllEntities().stream()
            .filter(entity -> entity.getEntityType() == EntityType.DATACENTER
                            || entity.getEntityType() == EntityType.PHYSICAL_MACHINE)
            .forEach(entity -> {
                if (entity.getEntityType() == EntityType.DATACENTER)    {
                    for (PerTargetInfo dcInfo : entity.allTargetInfo())    {
                        datacenterIDsToOIDs.put(dcInfo.getEntityInfo().getId(), entity.getId());
                    }
                } else {
                    hosts.add(entity);
                }
            });

        Map<Long, Long> host2datacenterOIDs = findPM2DCConnections(hosts, datacenterIDsToOIDs);
        discoveredGroupUploader.setPM2DCMap(host2datacenterOIDs);
        if (!host2datacenterOIDs.isEmpty()) {
            status.append(" ").append(host2datacenterOIDs.size())
                .append(" host to datacenter connections are registered.");
        }

        return StageResult.withResult(input)
            .andStatus(Status.success(status.toString()));
    }

    /**
     * Checks commodities bought by PMs and finds DCs they are buying from.
     * @param hosts Discovered physical machines.
     * @param datacenterIDsToOIDs   A map of SDK String IDs to XL OIDs for datacenters.
     * @return map of PM OIDs to provider DC OIDs
     */
    private Map<Long, Long> findPM2DCConnections(List<Entity> hosts,
                    Map<String, Long> datacenterIDsToOIDs) {
        Map<Long, Long> host2datacenterOIDs = new HashMap<>();
        for (Entity host : hosts)   {
            for (PerTargetInfo entityInfo : host.allTargetInfo())    {
                EntityDTO entityDTO = entityInfo.getEntityInfo();
                for (CommodityBought bought : entityDTO.getCommoditiesBoughtList()) {
                    if (bought.getProviderType() == EntityType.DATACENTER)  {
                        Long datacenterOID = datacenterIDsToOIDs.get(bought.getProviderId());
                        host2datacenterOIDs.put(host.getId(), datacenterOID);
                        break;
                    }
                }
            }
        }
        return host2datacenterOIDs;
    }
}