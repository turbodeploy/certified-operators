package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessUserData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Business User - i.e. {@link BusinessUserInfo}.
 **/
public class BusinessUserMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo
    mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                   @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasBusinessUserData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        BusinessUserData buData = sdkEntity.getBusinessUserData();
        BusinessUserInfo.Builder infoBuilder = BusinessUserInfo.newBuilder();
        for (final SessionData sessionData : buData.getSessionDataList()) {
            // the vm oid should be resolved at this point
            if (NumberUtils.isParsable(sessionData.getVirtualMachine())) {
                final long vmOid = Long.parseLong(sessionData.getVirtualMachine());
                infoBuilder.putVmOidToSessionDuration(vmOid, sessionData.getSessionDuration());
            } else {
                logger.debug("Virtual Machine {} has not been resolved to a VM OID" +
                        " in the VM stitching operation.", sessionData.getVirtualMachine());
            }
        }
        return TypeSpecificInfo.newBuilder().setBusinessUser(infoBuilder.build())
                .build();
    }

}
