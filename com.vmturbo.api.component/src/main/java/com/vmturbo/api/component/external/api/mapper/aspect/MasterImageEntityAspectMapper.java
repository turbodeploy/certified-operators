package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.MasterImageEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting master image aspect.
 */
public class MasterImageEntityAspectMapper extends AbstractAspectMapper {
    private static final Logger LOGGER = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     */
    public MasterImageEntityAspectMapper(final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        if (entity.getEntityType() == EntityType.DESKTOP_POOL_VALUE) {
            return mapDesktopPoolToAspect(entity);
        } else if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            final Optional<Long> desktopPoolOid = entity.getCommoditiesBoughtFromProvidersList()
                    .stream()
                    .filter(CommoditiesBoughtFromProvider::hasProviderId)
                    .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                    .filter(c -> c.getProviderEntityType() == EntityType.DESKTOP_POOL_VALUE)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .findFirst();
            if (desktopPoolOid.isPresent()) {
                return repositoryApi.entityRequest(desktopPoolOid.get())
                        .getFullEntity()
                        .map(this::mapDesktopPoolToAspect)
                        .orElse(null);
            } else {
                LOGGER.trace(
                        "Could not find desktop pool id by bought commodities virtual machine with oid '{}'",
                        entity::getOid);
            }
        }
        return null;
    }

    /**
     * Maps the {@link TopologyEntityDTO} that contains {@link DesktopPoolInfo}.
     *
     * @param entity the {@link TopologyEntityDTO} that contains {@link DesktopPoolInfo}
     * @return the {@link MasterImageEntityAspectApiDTO}
     */
    @Nullable
    private MasterImageEntityAspectApiDTO mapDesktopPoolToAspect(
            @Nonnull TopologyEntityDTO entity) {
        if (entity.hasTypeSpecificInfo()) {
            final TypeSpecificInfo info = entity.getTypeSpecificInfo();
            if (info.hasDesktopPool() && info.getDesktopPool().hasVmWithSnapshot()) {
                return repositoryApi.entityRequest(
                        info.getDesktopPool().getVmWithSnapshot().getVmReferenceId())
                        .getFullEntity()
                        .map(MasterImageEntityAspectMapper::createAspectByVirtualMachineDTO)
                        .orElse(null);
            }
        }
        // TODO: Implement case when clone source reference - template
        return null;
    }

    /**
     * Creates {@link MasterImageEntityAspectApiDTO}
     * by the {@link TopologyEntityDTO} that contains {@link VirtualMachineInfo}.
     *
     * @param entity the {@link TopologyEntityDTO} that contains {@link VirtualMachineInfo}
     * @return the {@link MasterImageEntityAspectApiDTO}
     */
    @Nullable
    private static MasterImageEntityAspectApiDTO createAspectByVirtualMachineDTO(
            @Nonnull TopologyEntityDTO entity) {
        if (!entity.hasTypeSpecificInfo() || !entity.getTypeSpecificInfo().hasVirtualMachine()) {
            return null;
        }
        final MasterImageEntityAspectApiDTO aspect = new MasterImageEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        final VirtualMachineInfo virtualMachineInfo = entity.getTypeSpecificInfo().getVirtualMachine();
        if (virtualMachineInfo.hasNumCpus()) {
            aspect.setNumVcpus(virtualMachineInfo.getNumCpus());
        }
        entity.getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.VMEM_VALUE)
                .findFirst()
                .ifPresent(vmem -> aspect.setMem((float)vmem.getCapacity()));
        final double storage = entity.getCommoditiesBoughtFromProvidersList()
                .stream()
                .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                .flatMap(Collection::stream)
                .filter(c -> c.getCommodityType().getType() ==
                        CommodityType.STORAGE_PROVISIONED_VALUE)
                .mapToDouble(CommodityBoughtDTO::getUsed)
                .sum();
        aspect.setStorage((float)storage);
        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.MASTER_IMAGE;
    }
}
