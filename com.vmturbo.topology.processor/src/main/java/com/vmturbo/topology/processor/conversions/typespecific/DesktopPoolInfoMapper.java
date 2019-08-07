package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a desktop pool - i.e. {@link DesktopPoolInfo}.
 **/
public class DesktopPoolInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {

        if (!sdkEntity.hasVirtualDatacenterData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final VirtualDatacenterData vdcData = sdkEntity.getVirtualDatacenterData();
        if (!vdcData.hasDesktopPoolData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final DesktopPoolData dpData = vdcData.getDesktopPoolData();

        final DesktopPoolInfo.Builder desktopInfoBuilder = DesktopPoolInfo.newBuilder();
        final DesktopPoolAssignmentType assignmentType = dpData.getAssignmentType();
        final DesktopPoolProvisionType provisionType = dpData.getProvisionType();
        final DesktopPoolCloneType cloneType = dpData.getCloneType();

        if (dpData.hasMasterImage()) {
            // the vm oid should be resolved at this point
            if (NumberUtils.isParsable(dpData.getMasterImage())) {
                final long vmOid = Long.parseLong(dpData.getMasterImage());
                desktopInfoBuilder.setVmReferenceId(vmOid);
            } else {
                logger.debug("Master Image {} has not been resolved to a VM OID." +
                        " It is possible that this is a template UID" +
                        " to be resolved during the template upload. ", dpData.getMasterImage());
            }
        }
        return TypeSpecificInfo.newBuilder()
                        .setDesktopPool(
                                desktopInfoBuilder.setAssignmentType(assignmentType)
                                        .setProvisionType(provisionType)
                                        .setCloneType(cloneType)
                        .build()).build();
    }
}
