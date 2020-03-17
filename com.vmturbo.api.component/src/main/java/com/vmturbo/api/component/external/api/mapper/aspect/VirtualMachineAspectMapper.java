package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.api.dto.user.BusinessUserSessionApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;

/**
 * Topology Extension data related to Virtual Machine.
 **/
public class VirtualMachineAspectMapper extends AbstractAspectMapper {
    private final RepositoryApi repositoryApi;

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     */
    public VirtualMachineAspectMapper(final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final VMEntityAspectApiDTO aspect = new VMEntityAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasVirtualMachine()) {
            final VirtualMachineInfo virtualMachineInfo = entity.getTypeSpecificInfo()
                .getVirtualMachine();
            if (!virtualMachineInfo.getIpAddressesList().isEmpty()) {
                aspect.setIp(virtualMachineInfo
                    .getIpAddressesList()
                    .stream().map(IpAddress::getIpAddress)
                    .collect(Collectors.toList()));
            }
            if (virtualMachineInfo.hasGuestOsInfo()) {
                aspect.setOs(virtualMachineInfo.getGuestOsInfo().getGuestOsName());
            }
            if (virtualMachineInfo.hasNumCpus()) {
                aspect.setNumVCPUs(virtualMachineInfo.getNumCpus());
            }
            if (!virtualMachineInfo.getConnectedNetworksList().isEmpty()) {
                aspect.setConnectedNetworks(virtualMachineInfo.getConnectedNetworksList().stream()
                    .map(this::mapToNetwork).collect(Collectors.toList()));
            }
            aspect.setSessions(getBusinessUserSessions(entity));
            final String isEbsOptimized =
                    entity.getEntityPropertyMapMap().get(StringConstants.EBS_OPTIMIZED);
            if (isEbsOptimized != null) {
                aspect.setEbsOptimized(Boolean.parseBoolean(isEbsOptimized));
            }

            if (virtualMachineInfo.getLicenseModel() == LicenseModel.AHUB) {
                aspect.setAHUBLicense(true);
            }
        }
        return aspect;
    }

    /**
     * Find all sessions established between the business user and the virtual machine
     * and creates {@link BusinessUserSessionApiDTO} for each session.
     *
     * @param entity the {@link TopologyEntityDTO}
     * @return the list of {@link BusinessUserSessionApiDTO}
     */
    private List<BusinessUserSessionApiDTO> getBusinessUserSessions(
            @Nonnull final TopologyEntityDTO entity) {
        final List<BusinessUserSessionApiDTO> sessions = new LinkedList<>();
        repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(entity.getOid(), TraversalDirection.PRODUCES,
                        UIEntityType.BUSINESS_USER))
                .getFullEntities()
                .filter(e -> e.hasTypeSpecificInfo() && e.getTypeSpecificInfo().hasBusinessUser())
                .forEach((e) -> {
                    final Long sessionDuration = e.getTypeSpecificInfo()
                            .getBusinessUser()
                            .getVmOidToSessionDurationMap()
                            .get(entity.getOid());
                    if (sessionDuration != null) {
                        final BusinessUserSessionApiDTO dto = new BusinessUserSessionApiDTO();
                        dto.setBusinessUserUuid(String.valueOf(e.getOid()));
                        dto.setConnectedEntityUuid(String.valueOf(entity.getOid()));
                        dto.setDuration(sessionDuration);
                        sessions.add(dto);
                    }
                });
        return sessions;
    }

    /**
     * Maps the connected network name into a BaseApiDTO.
     *
     * @param networkName the underlying network display name
     * @return the BaseApiDTO representing it.
     */
    @Nonnull
    private BaseApiDTO mapToNetwork(final @Nonnull String networkName) {
        final BaseApiDTO dto = new BaseApiDTO();
        dto.setDisplayName(networkName);
        return dto;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.VIRTUAL_MACHINE;
    }
}
