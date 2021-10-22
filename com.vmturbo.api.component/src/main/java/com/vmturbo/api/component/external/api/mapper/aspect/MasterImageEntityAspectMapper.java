package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.MasterImageEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting master image aspect.
 */
public class MasterImageEntityAspectMapper extends AbstractAspectMapper {
    private static final Logger LOGGER = LogManager.getLogger();

    private final RepositoryApi repositoryApi;
    private final TemplateServiceBlockingStub templateService;

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     * @param templateService provides access to templates
     */
    public MasterImageEntityAspectMapper(final RepositoryApi repositoryApi,
                    TemplateServiceBlockingStub templateService) {
        this.repositoryApi = repositoryApi;
        this.templateService = templateService;
    }

    @Nullable
    @Override
    public MasterImageEntityAspectApiDTO mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
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
        if (entity.hasTypeSpecificInfo() && entity.getTypeSpecificInfo().hasDesktopPool()) {
            final DesktopPoolInfo info = entity.getTypeSpecificInfo().getDesktopPool();
            if (info.hasVmWithSnapshot()) {
                final long vmReferenceId = info.getVmWithSnapshot().getVmReferenceId();
                TopologyEntityDTO vm = repositoryApi.entityRequest(vmReferenceId)
                        .getFullEntity()
                        .orElse(null);
                if (vm == null) {
                    LOGGER.warn(
                            "The VM could not be found by id {} when collecting information for entity {}",
                            vmReferenceId, entity.getOid());
                    return null;
                }
                return MasterImageEntityAspectMapper.createAspectByDpVmPmDTOs(entity, vm,
                        getVmProvider(vm));
            } else if (info.hasTemplateReferenceId()) {
                try {
                    SingleTemplateResponse response = templateService.getTemplate(GetTemplateRequest
                                    .newBuilder().setTemplateId(info.getTemplateReferenceId())
                                    .build());
                    if (response.hasTemplate()) {
                        return createAspectByTemplate(response.getTemplate());
                    } else {
                        LOGGER.error("Could not find template by id " + info.getTemplateReferenceId());
                    }
                } catch (StatusRuntimeException e) {
                    // no exception handling across aspects
                    LOGGER.error("Failed to retrieve template by id " + info.getTemplateReferenceId(), e);
                }
            } else {
                LOGGER.error("Master image is not referenced by a VDI entity " + entity.getDisplayName());
            }
        }
        return null;
    }

    private TopologyEntityDTO getVmProvider(TopologyEntityDTO vm) {
        CommoditiesBoughtFromProvider provider =
                vm.getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .filter(p -> p.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                        .findFirst()
                        .orElse(null);
        if (provider != null) {
            final long providerId = provider.getProviderId();
            final TopologyEntityDTO pm = repositoryApi.entityRequest(providerId)
                    .getFullEntity()
                    .orElse(null);
            if (pm != null) {
                return pm;
            }
        }
        String providerIdInfo = provider != null ? "by id " + provider.getProviderId() : "";
        LOGGER.warn("The provider PM could not be found {} for VM {}", providerIdInfo, vm.getOid());
        return null;
    }

    private static MasterImageEntityAspectApiDTO createAspectByTemplate(Template template) {
        final MasterImageEntityAspectApiDTO aspect = new MasterImageEntityAspectApiDTO();
        String description = template.getTemplateInfo().getDescription();
        aspect.setDisplayName(StringUtils.isBlank(description)
                        ? template.getTemplateInfo().getName()
                        : description);
        aspect.setNumVcpus((int)getField(template.getTemplateInfo(),
                                    ResourcesCategoryName.Compute,
                                    TemplatesUtils.NUM_OF_CPU));
        aspect.setMem(getField(template.getTemplateInfo(),
                                    ResourcesCategoryName.Compute,
                                    TemplatesUtils.MEMORY_SIZE));
        aspect.setStorage(getField(template.getTemplateInfo(),
                               ResourcesCategoryName.Storage,
                               TemplatesUtils.DISK_SIZE));
        return aspect;
    }

    private static float getField(TemplateInfo template, ResourcesCategoryName cat,
                                  String fieldName) {
        Set<String> values = template.getResourcesList().stream()
                        .filter(resource -> resource.getCategory().getName() == cat)
                        .flatMap(resource -> resource.getFieldsList().stream())
                        .filter(field -> fieldName.equals(field.getName()))
                        .map(TemplateField::getValue)
                        .collect(Collectors.toSet());
        float result = 0F;
        for (String value : values) {
            try {
                result += Float.valueOf(value);
            } catch (NumberFormatException e) {
                LOGGER.warn("Not a numeric value for template {} field {}: {}",
                            template.getDescription(), fieldName, value);
            }
        }
        return result;
    }

    /**
     * Creates {@link MasterImageEntityAspectApiDTO}
     * by the desktop pool, virtual machine and physical machine {@link TopologyEntityDTO} .
     *
     * @param dp the desktop pool {@link TopologyEntityDTO}
     * @param vm the virtual machine {@link TopologyEntityDTO} that contains {@link
     *         VirtualMachineInfo}
     * @param pm the physical machine {@link TopologyEntityDTO}
     * @return the {@link MasterImageEntityAspectApiDTO}
     */
    @Nonnull
    private static MasterImageEntityAspectApiDTO createAspectByDpVmPmDTOs(
            @Nonnull TopologyEntityDTO dp, @Nonnull TopologyEntityDTO vm,
            @Nullable TopologyEntityDTO pm) {
        final MasterImageEntityAspectApiDTO aspect = new MasterImageEntityAspectApiDTO();
        final VmWithSnapshot vmWithSnapshot =
                dp.getTypeSpecificInfo().getDesktopPool().getVmWithSnapshot();
        final String snapshotNameInfo =
                vmWithSnapshot.hasSnapshot() ? " (" + vmWithSnapshot.getSnapshot() + ")" : "";
        aspect.setDisplayName(vm.getDisplayName() + snapshotNameInfo);
        dp.getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.IMAGE_MEM_VALUE)
                .findFirst()
                .ifPresent(mem -> aspect.setMem((float)mem.getCapacity()));
        CommoditySoldDTO imageCpuValue = dp.getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.IMAGE_CPU_VALUE)
                .findFirst()
                .orElse(null);
        if (pm != null) {
            TypeSpecificInfo typeSpecificInfo = pm.getTypeSpecificInfo();
            if (imageCpuValue != null && pm.hasTypeSpecificInfo()
                    && typeSpecificInfo.hasPhysicalMachine()) {
                int cpuCoreMhz = typeSpecificInfo.getPhysicalMachine().getCpuCoreMhz();
                if (cpuCoreMhz > 0) {
                    aspect.setNumVcpus((int)Math.round(imageCpuValue.getCapacity() / cpuCoreMhz));
                }
            }
        }
        final double storage = vm.getCommoditiesBoughtFromProvidersList()
                .stream()
                .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                .flatMap(Collection::stream)
                .filter(c -> c.getCommodityType().getType()
                        == CommodityType.STORAGE_PROVISIONED_VALUE)
                .mapToDouble(CommodityBoughtDTO::getUsed)
                .sum();
        aspect.setStorage((float)storage);
        return aspect;
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatch(
        @Nonnull List<TopologyEntityDTO> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatchPartial(
        @Nonnull List<ApiPartialEntity> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.MASTER_IMAGE;
    }
}
