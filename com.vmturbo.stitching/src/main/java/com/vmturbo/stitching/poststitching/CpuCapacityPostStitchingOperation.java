package com.vmturbo.stitching.poststitching;

import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting CPU commodity capacities for physical
 * machines if not already set.
 *
 * If the entity in question has a CPU commodity with unset capacity and an entity properties map
 * that includes properties for number of CPU cores and CPU core MHz, then the CPU commodity's
 * capacity is set to the CPU core MHz multiplied by the number of cores.
 */
public class CpuCapacityPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    private final static String NUM_CPU_CORES = "common_dto.EntityDTO.PhysicalMachineData.numCpuCores";
    private final static String CPU_CORE_MHZ = "common_dto.EntityDTO.PhysicalMachineData.cpuCoreMhz";

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(entity -> {
            final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
            final Map<String, String> props = entityBuilder.getEntityPropertyMapMap();
            final boolean hasNumCoresProperty = props.containsKey(NUM_CPU_CORES);
            final boolean hasCpuMhzProperty = props.containsKey(CPU_CORE_MHZ);
            final boolean needsUpdate = entityBuilder.getCommoditySoldListBuilderList().stream()
                .anyMatch(this::hasSettableCpuCapacity);

            if (hasNumCoresProperty && hasCpuMhzProperty && needsUpdate) {
                resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
                    entityForUpdate.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                        .filter(this::hasSettableCpuCapacity)
                        .forEach(commodity -> {
                            final double numCores = Double.valueOf(props.get(NUM_CPU_CORES));
                            final double cpuMhz = Double.valueOf(props.get(CPU_CORE_MHZ));
                            commodity.setCapacity(numCores * cpuMhz);
                            logger.trace("Entity {} CPU commodity capacity set to {} ({} cores @ {} MHz)",
                                entity.getOid(), commodity.getCapacity(), numCores, cpuMhz);
                        });
                });
            } else if (needsUpdate) {
                final String missing;
                if (hasCpuMhzProperty) {
                    missing = NUM_CPU_CORES;
                } else if (hasNumCoresProperty) {
                    missing = CPU_CORE_MHZ;
                } else {
                    missing = NUM_CPU_CORES + " and property " + CPU_CORE_MHZ;
                }
                logger.warn("Entity {} is missing property {} ; CPU commodity capacity was not set.",
                    entity.getOid(), missing);
            }
        });
        return resultBuilder.build();
    }

    /**
     * If the commodity is CPU and has unset capacity (which sometimes presents as capacity == 0)
     *
     * @param commodity the commodity to check
     * @return true if the capacity is settable, false otherwise
     */
    private boolean hasSettableCpuCapacity(@Nonnull final Builder commodity) {
        return commodity.getCommodityType().getType() == CommodityType.CPU_VALUE &&
            (!commodity.hasCapacity() || commodity.getCapacity() == 0);
    }
}
