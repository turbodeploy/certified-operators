package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot.Builder;
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
    /**
     * Entity property name to contain the raw external template identifier.
     */
    public static final String DESKTOP_POOL_TEMPLATE_REFERENCE = "desktopPoolTemplateReference";

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
                final Builder vmWithSnapshotBuilder = VmWithSnapshot.newBuilder();
                vmWithSnapshotBuilder.setVmReferenceId(vmOid);
                if (dpData.hasSnapshot()) {
                    vmWithSnapshotBuilder.setSnapshot(dpData.getSnapshot());
                }
                desktopInfoBuilder.setVmWithSnapshot(vmWithSnapshotBuilder.build());
            } else {
                // template identities are resolved much later
                // this hack is to pass the raw vendor template identifier reference down the
                // data flow, in a temporary entity property, to get converted after
                // DiscoveredTemplateDeploymentProfileUploader is done
                // TODO alternatives:
                // - (not good either) invert the relationship, reference "1-to-many" template to desktop pools
                // - (somewhat better) turn all stitching, the type-specific mappers, SdkToTopologyEntityConverter etc to beans,
                //   StitchingContext to prototype, inject DiscoveredTemplateDeploymentProfileUploader here
                //   and register template id patchers in it for later execution
                // - (better) resolve all the identities - entities, templates, groups, non-market entities etc
                //   uniformly and simultaneously before stitching, then connecting pool to template could be
                //   a legitimate stitcher
                entityPropertyMap.put(DESKTOP_POOL_TEMPLATE_REFERENCE, dpData.getMasterImage());

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
