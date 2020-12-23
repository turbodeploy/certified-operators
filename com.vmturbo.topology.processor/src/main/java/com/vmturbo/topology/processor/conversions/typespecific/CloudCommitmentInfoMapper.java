package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a CloudCommitment - i.e. {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo}
 */
public class CloudCommitmentInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasCloudCommitmentData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final CloudCommitmentData cloudCommitmentData = sdkEntity.getCloudCommitmentData();
        Builder cloudCommitmentInfoBuilder = TypeSpecificInfo.CloudCommitmentInfo.newBuilder();
        switch (cloudCommitmentData.getScopeCase()) {
            case SERVICE_RESTRICTED:
                cloudCommitmentInfoBuilder.setServiceRestricted(cloudCommitmentData.getServiceRestricted());
                break;
            case FAMILY_RESTRICTED:
                cloudCommitmentInfoBuilder.setFamilyRestricted(cloudCommitmentData.getFamilyRestricted());
                break;
            default:
                logger.error("No scope found on Cloud Commitment Data {}", cloudCommitmentData.toString());
                break;
        }
        switch (cloudCommitmentData.getCommitmentCase()) {
            case SPEND:
                cloudCommitmentInfoBuilder.setSpend(cloudCommitmentData.getSpend());
                break;
            case NUMBER_COUPONS:
                cloudCommitmentInfoBuilder.setNumberCoupons(cloudCommitmentData.getNumberCoupons());
                break;
            default:
                logger.error("No commitment found on Cloud Commitment Data {}", cloudCommitmentData.toString());
        }
        if (cloudCommitmentData.hasExpirationTimeMilliseconds()) {
            cloudCommitmentInfoBuilder.setExpirationTimeMilliseconds(cloudCommitmentData.getExpirationTimeMilliseconds());
        }
        if (cloudCommitmentData.hasPayment()) {
            cloudCommitmentInfoBuilder.setPayment(cloudCommitmentData.getPayment());
        }
        if (cloudCommitmentData.hasStartTimeMilliseconds()) {
            cloudCommitmentInfoBuilder.setStartTimeMilliseconds(cloudCommitmentData.getStartTimeMilliseconds());
        }
        if (cloudCommitmentData.hasTermMilliseconds()) {
            cloudCommitmentInfoBuilder.setTermMilliseconds(cloudCommitmentData.getTermMilliseconds());
        }
        return TypeSpecificInfo.newBuilder().setCloudCommitmentData(cloudCommitmentInfoBuilder.build())
                .build();
    }
}
