package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entityaspect.DesktopPoolEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.DesktopPoolAssignmentType;
import com.vmturbo.api.enums.DesktopPoolCloneType;
import com.vmturbo.api.enums.DesktopPoolProvisionType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * Mapper for getting desktop pool aspect.
 */
public class DesktopPoolAspectMapper extends AbstractAspectMapper {
    private final RepositoryApi repositoryApi;
    private final GroupServiceBlockingStub groupServiceBlockingStub;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     * @param groupServiceBlockingStub the {@link GroupServiceBlockingStub}
     */
    public DesktopPoolAspectMapper(final RepositoryApi repositoryApi,
            final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.repositoryApi = repositoryApi;
        this.groupServiceBlockingStub = groupServiceBlockingStub;
    }

    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        if (entity.getEntityType() == EntityType.DESKTOP_POOL_VALUE) {
            return mapDesktopPoolToAspect(entity);
        } else if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            final Optional<Long> cpuAllocationProviderId = getCPUAllocationProviderId(entity);
            if (cpuAllocationProviderId.isPresent()) {
                return repositoryApi.entityRequest(cpuAllocationProviderId.get())
                        .getFullEntity()
                        .map(this::mapDesktopPoolToAspect)
                        .orElse(null);
            }
        }
        return null;
    }

    /**
     * Maps the {@link TopologyEntityDTO} that contains {@link DesktopPoolInfo}.
     *
     * @param entity the {@link TopologyEntityDTO} that contains {@link DesktopPoolInfo}
     * @return the {@link DesktopPoolEntityAspectApiDTO}
     */
    @Nullable
    private DesktopPoolEntityAspectApiDTO mapDesktopPoolToAspect(
            @Nonnull TopologyEntityDTO entity) {
        if (!entity.hasTypeSpecificInfo() || !entity.getTypeSpecificInfo().hasDesktopPool()) {
            return null;
        }
        final DesktopPoolEntityAspectApiDTO aspect = new DesktopPoolEntityAspectApiDTO();
        final DesktopPoolInfo desktopPoolInfo = entity.getTypeSpecificInfo().getDesktopPool();
        if (desktopPoolInfo.hasAssignmentType()) {
            aspect.setAssignmentType(
                    DesktopPoolAssignmentType.valueOf(desktopPoolInfo.getAssignmentType().name()));
        }
        if (desktopPoolInfo.hasCloneType()) {
            aspect.setCloneType(
                    DesktopPoolCloneType.valueOf(desktopPoolInfo.getCloneType().name()));
        }
        if (desktopPoolInfo.hasProvisionType()) {
            aspect.setProvisionType(
                    DesktopPoolProvisionType.valueOf(desktopPoolInfo.getProvisionType().name()));
        }
        if (desktopPoolInfo.hasTemplateReferenceId()) {
            aspect.setMasterTemplateUuid(String.valueOf(desktopPoolInfo.getTemplateReferenceId()));
        }
        if (desktopPoolInfo.hasVmWithSnapshot()) {
            final VmWithSnapshot vmWithSnapshot = desktopPoolInfo.getVmWithSnapshot();
            aspect.setMasterVirtualMachineUuid(String.valueOf(vmWithSnapshot.getVmReferenceId()));
            if (vmWithSnapshot.hasSnapshot()) {
                aspect.setMasterVirtualMachineSnapshot(vmWithSnapshot.getSnapshot());
            }
        }
        aspect.setVendorId(entity.getEntityPropertyMapMap().get(SDKUtil.VENDOR_ID));
        final Optional<MinimalEntity> physicalMachine = repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(SearchProtoUtil.idFilter(entity.getOid()))
                        .addSearchFilter(SearchFilter.newBuilder()
                                .setTraversalFilter(
                                        SearchProtoUtil.traverseToType(TraversalDirection.CONSUMES,
                                                UIEntityType.PHYSICAL_MACHINE.apiStr())))
                        .build()).getMinimalEntities().findFirst();
        if (physicalMachine.isPresent()) {
            final long pmId = physicalMachine.get().getOid();
            final GetGroupsForEntitiesResponse response =
                    groupServiceBlockingStub.getGroupsForEntities(
                            GetGroupsForEntitiesRequest.newBuilder()
                                    .addEntityId(pmId)
                                    .addGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                                    .setLoadGroupObjects(true)
                                    .build());
            // response contains only clusters related to this entity. It is expected that
            // there will be only one cluster
            final Optional<String> clusterName = response.getGroupsList()
                    .stream()
                    .findAny()
                    .map(group -> group.getDefinition().getDisplayName());
            if (clusterName.isPresent()) {
                aspect.setvCenterClusterName(clusterName.get());
            }
        }
        return aspect;
    }

    private static Optional<Long> getCPUAllocationProviderId(@Nonnull TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .filter((c) -> c.getCommodityBoughtList()
                        .stream()
                        .anyMatch(commodity -> commodity.getCommodityType().getType() ==
                                CommodityType.CPU_ALLOCATION_VALUE))
                        .sorted((entity1, entity2) -> {
                            // We expect only one desktop pool for VM and it has more priority than VDC
                            if (entity1.getProviderEntityType() == EntityType.DESKTOP_POOL_VALUE) {
                                return -1;
                            } else if (entity2.getProviderEntityType()
                                       == EntityType.DESKTOP_POOL_VALUE) {
                                return 1;
                            } else {
                                return 0;
                            }
                        })
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .findFirst();
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DESKTOP_POOL;
    }
}
