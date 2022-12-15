package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;

/**
 * This stage checks if there're Fabric targets in entityStore, and if there're
 * then makes a linear search entityStore, gets PMs and saves info about their
 * datacenter display names in {@link DiscoveredGroupUploader}.
 * TODO: This stage should be checked and possibly removed after APP-734 implementation.
 */
public class DCMappingStage extends Stage<PipelineInput, PipelineInput> {
    private final DiscoveredGroupUploader discoveredGroupUploader;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor for the stage.
     * @param groupUploader a {@link DiscoveredGroupUploader} instance that will be
     *      keeping the desired data.
     */
    public DCMappingStage(final DiscoveredGroupUploader groupUploader) {
        this.discoveredGroupUploader = groupUploader;
    }

    @Nonnull
    @Override
    public StageResult<PipelineInput> executeStage(@Nonnull PipelineInput input)
                    throws PipelineStageException, InterruptedException {
        if (!discoveredGroupUploader.isFabricTargetPresent())   {
            return StageResult.withResult(input)
                .andStatus(Status.success("No Fabric targets. Skipping DCMappingStage."));
        }

        // If Feature Flag is disabled (default), execute stage in the original way.
        if (!FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT.isEnabled()) {
            EntityStore entityStore = input.getEntityStore();
            StringBuilder status = new StringBuilder("Fabric targets are present.");
            // A map of SDK String IDs to DC name:
            Map<String, String> datacenterIDsToNames = new HashMap<>();
            List<Entity> hosts = new ArrayList<>();
            entityStore.getAllEntities().stream()
                 .filter(entity -> entity.getEntityType() == EntityType.DATACENTER
                         || entity.getEntityType() == EntityType.PHYSICAL_MACHINE)
                 .forEach(entity -> {
                     if (entity.getEntityType() == EntityType.DATACENTER) {
                         for (PerTargetInfo dcInfo : entity.allTargetInfo()) {
                             try {
                                 EntityDTO entityDTO = dcInfo.getEntityInfo();
                                 datacenterIDsToNames.put(entityDTO.getId(), entityDTO.getDisplayName());
                             } catch (InvalidProtocolBufferException e) {
                                 logger.error(entity.getConversionError());
                             }
                         }
                     } else {
                         hosts.add(entity);
                     }
                 });

            final Map<Long, String> host2datacenterName = createPM2DCNameMap(hosts, datacenterIDsToNames);
            discoveredGroupUploader.setPM2DCNameMap(host2datacenterName);
            if (!host2datacenterName.isEmpty()) {
                status.append(" ").append(host2datacenterName.size())
                      .append(" host to datacenter names are registered.");
            }

            return StageResult.withResult(input)
                              .andStatus(Status.success(status.toString()));
        }

        // If Feature Flag is enabled, execute stage with Stitching Context as input.
        StitchingContext stitchingContext = input.getStitchingContext();
        StringBuilder status = new StringBuilder("Fabric targets are present.");

        List<TopologyStitchingEntity> hosts = stitchingContext
                .getEntitiesOfType(EntityType.PHYSICAL_MACHINE)
                .collect(Collectors.toList());

        // A map of SDK IDs to DC name:
        Map<Long, String> datacenterIDsToNames = stitchingContext
                .getEntitiesOfType(EntityType.DATACENTER)
                .collect(Collectors.toMap(TopologyStitchingEntity::getOid,
                        TopologyStitchingEntity::getDisplayName));

        final Map<Long, String> host2datacenterName = createHost2DCNameMap(hosts, datacenterIDsToNames);
        discoveredGroupUploader.setPM2DCNameMap(host2datacenterName);
        if (!host2datacenterName.isEmpty()) {
            status.append(" ").append(host2datacenterName.size())
                  .append(" host to datacenter names are registered.");
        }

        return StageResult.withResult(input)
                          .andStatus(Status.success(status.toString()));
    }

    /**
     * Checks commodities bought by PMs and finds DCs they are buying from.
     *
     * @param hosts Discovered physical machines.
     * @param datacenterIDsToDisplayNames A map of SDK String IDs to datacenter display names.
     * @return map of PM OIDs to provider DC display name
     */
    private static Map<Long, String> createPM2DCNameMap(List<Entity> hosts,
            Map<String, String> datacenterIDsToDisplayNames) {
        if (FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT.isEnabled()) {
            throw new IllegalStateException(
                    "This method must only be called with USE_EXTENDABLE_PIPELINE_INPUT Feature Flag disabled.");
        }
        Map<Long, String> host2datacenterNames = new HashMap<>();
        for (Entity host : hosts)   {
            for (PerTargetInfo entityInfo : host.allTargetInfo()) {
                try {
                    EntityDTO entityDTO = entityInfo.getEntityInfo();
                    for (CommodityBought bought : entityDTO.getCommoditiesBoughtList()) {
                        if (bought.getProviderType() == EntityType.DATACENTER)  {
                            final String datacenterName = datacenterIDsToDisplayNames.get(bought.getProviderId());
                            host2datacenterNames.put(host.getId(), datacenterName);
                            break;
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    logger.error(host.getConversionError(), e);
                }
            }
        }
        return host2datacenterNames;
    }

    /**
     * Checks commodities bought by PMs and finds DCs they are buying from.
     * Overloaded method for list of {@link TopologyStitchingEntity}.
     *
     * @param hosts Discovered physical machines.
     * @param datacenterIDsToDisplayNames A map of SDK String IDs to datacenter display names.
     * @return map of PM OIDs to provider DC display name
     */
    private static Map<Long, String> createHost2DCNameMap(List<TopologyStitchingEntity> hosts,
            Map<Long, String> datacenterIDsToDisplayNames) {
        if (!FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT.isEnabled()) {
            throw new IllegalStateException(
                    "This method must only be called with USE_EXTENDABLE_PIPELINE_INPUT Feature Flag enabled.");
        }
        Map<Long, String> host2datacenterNames = new HashMap<>();
        for (TopologyStitchingEntity host : hosts) {
            for (StitchingEntity provider : host.getProviders()) {
                if (provider.getEntityType() == EntityType.DATACENTER) {
                    final String datacenterName =
                            datacenterIDsToDisplayNames.get(provider.getOid());
                    host2datacenterNames.put(host.getOid(), datacenterName);
                    break;
                }
            }
        }
        return host2datacenterNames;
    }
}