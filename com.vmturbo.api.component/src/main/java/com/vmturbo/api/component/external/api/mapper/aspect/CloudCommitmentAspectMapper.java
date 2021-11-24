package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.cloudcommitment.CloudCommitmentCapacityApiDTO;
import com.vmturbo.api.dto.cloudcommitment.CloudCommitmentScopeDTO;
import com.vmturbo.api.dto.cloudcommitment.CloudFamilyReferenceApiDTO;
import com.vmturbo.api.dto.entityaspect.CloudCommitmentAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.CloudCommitmentScopeType;
import com.vmturbo.api.enums.CloudCommitmentStatus;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.ProviderType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost;

/**
 * The cloud commitment aspect mapper is used to map entity info about cloud commitments to cloud commitment
 * aspect api dtos.
 */
public class CloudCommitmentAspectMapper extends AbstractAspectMapper {
    private static final Logger logger = LogManager.getLogger();

    @Nullable
    @Override
    public CloudCommitmentAspectApiDTO mapEntityToAspect(@Nonnull TopologyEntityDTO entity)
            throws InterruptedException, ConversionException {
        if (entity.getEntityType() != EntityType.CLOUD_COMMITMENT_VALUE) {
            return null;
        }
        final CloudCommitmentInfo cloudCommitmentData = entity.getTypeSpecificInfo().getCloudCommitmentData();
        CloudCommitmentAspectApiDTO cloudCommitmentAspectApiDTO = new CloudCommitmentAspectApiDTO();
        PaymentOption paymentOption = convertPaymentToApiDTO(cloudCommitmentData.getPayment());
        if (paymentOption != null) {
            cloudCommitmentAspectApiDTO.setPayment(paymentOption);
        }
        cloudCommitmentAspectApiDTO.setStartTimeInMilliseconds(cloudCommitmentData.getStartTimeMilliseconds());
        cloudCommitmentAspectApiDTO.setExpirationTimeInMilliseconds(cloudCommitmentData.getExpirationTimeMilliseconds());
        cloudCommitmentAspectApiDTO.setTermInMilliseconds(cloudCommitmentData.getTermMilliseconds());
        cloudCommitmentAspectApiDTO.setCloudCommitmentCapacityApiDTO(createCloudCommitmentCapacityApiDTO(cloudCommitmentData));
        cloudCommitmentAspectApiDTO.setCloudCommitmentScopeType(cloudCommitmentData.hasFamilyRestricted()
                ? CloudCommitmentScopeType.FamilyScoped : CloudCommitmentScopeType.CloudServiceScoped);
        cloudCommitmentAspectApiDTO.setCloudCommitmentScopeDTO(createCloudCommitmentScopeDTO(cloudCommitmentData));
        ProviderType providerType = convertProviderTypeToApiDTO(cloudCommitmentData.getProviderSpecificType());
        if (providerType != null) {
            cloudCommitmentAspectApiDTO.setProviderSpecificType(providerType);
        }
        cloudCommitmentAspectApiDTO.setCommitmentStatus(convertStatusToAPiDTO(cloudCommitmentData.getCommitmentStatus()));
        return cloudCommitmentAspectApiDTO;
    }

    private CloudCommitmentStatus convertStatusToAPiDTO(
            @Nonnull CloudCommitmentData.CloudCommitmentStatus commitmentStatus) {
        switch (commitmentStatus) {
            case CLOUD_COMMITMENT_STATUS_ACTIVE:
                return CloudCommitmentStatus.Active;
            case CLOUD_COMMITMENT_STATUS_CANCELED:
                return CloudCommitmentStatus.Canceled;
            case CLOUD_COMMITMENT_STATUS_EXPIRED:
                return CloudCommitmentStatus.Expired;
            case CLOUD_COMMITMENT_STATUS_PENDING:
                return CloudCommitmentStatus.Pending;
            case CLOUD_COMMITMENT_STATUS_UNKNOWN:
                return CloudCommitmentStatus.Unknown;
            default:
                logger.error("Can not find matched payment commitment status: " + commitmentStatus);
                return null;
        }

    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.CLOUD_COMMITMENT;
    }

    /**
     * Convert {@link CommonCost.PaymentOption} to {@link PaymentOption}.
     *
     * @param paymentOption a {@link CommonCost.PaymentOption}.
     * @return a {@link PaymentOption}.
     */
    private PaymentOption convertPaymentToApiDTO(
            @Nonnull final CommonCost.PaymentOption paymentOption) {
        switch (paymentOption) {
            case ALL_UPFRONT:
                return PaymentOption.ALL_UPFRONT;
            case PARTIAL_UPFRONT:
                return PaymentOption.PARTIAL_UPFRONT;
            case NO_UPFRONT:
                return PaymentOption.NO_UPFRONT;
            default:
                logger.error("Can not find matched payment option: " + paymentOption);
                return null;
        }
    }

    /**
     * Convert {@link CloudCommitmentData.ProviderType} to {@link ProviderType}.
     *
     * @param providerType a {@link CloudCommitmentData.ProviderType}.
     * @return a {@link ProviderType}.
     */
    private ProviderType convertProviderTypeToApiDTO(
            @Nonnull final CloudCommitmentData.ProviderType providerType) {
        switch (providerType) {
            case SAVINGS_PLAN:
                return ProviderType.SAVINGS_PLAN;
            case COMMITTED_USE:
                return ProviderType.COMMITTED_USE;
            default:
                logger.error("Can not find matched provider type: {}", providerType);
                return null;
        }
    }

    private CloudCommitmentCapacityApiDTO createCloudCommitmentCapacityApiDTO(CloudCommitmentInfo cloudCommitmentData) {
        CloudCommitmentCapacityApiDTO cloudCommitmentCapacityApiDTO = new CloudCommitmentCapacityApiDTO();
        if (cloudCommitmentData.hasNumberCoupons()) {
            cloudCommitmentCapacityApiDTO.setInstanceCapacity(cloudCommitmentData.getNumberCoupons());
        }
        if (cloudCommitmentData.hasSpend()) {
            cloudCommitmentCapacityApiDTO.setSpendCapacity(cloudCommitmentData.getSpend().getAmount());
        }
        if (cloudCommitmentData.hasCommoditiesBought()) {

            cloudCommitmentData.getCommoditiesBought().getCommodityList().stream().forEach(commodity -> {
                cloudCommitmentCapacityApiDTO.getCommoditiesBought().put(CommodityType.valueOf(commodity.getCommodityType().name()), commodity.getCapacity());
            });
        }
        return cloudCommitmentCapacityApiDTO;
    }

    private CloudCommitmentScopeDTO createCloudCommitmentScopeDTO(CloudCommitmentInfo cloudCommitmentData) {
        CloudCommitmentScopeDTO cloudCommitmentScopeDTO = new CloudCommitmentScopeDTO();
        // If a cloud commitment is family restricted, only then we will set the CloudFamilyReferenceApiDTO.
        if (cloudCommitmentData.hasFamilyRestricted()) {
            CloudFamilyReferenceApiDTO cloudFamilyReferenceApiDTO = new CloudFamilyReferenceApiDTO();
            cloudFamilyReferenceApiDTO.setFamily(cloudCommitmentData.getFamilyRestricted().getInstanceFamily());
            cloudFamilyReferenceApiDTO.setEntityType(com.vmturbo.api.enums.EntityType.VirtualMachine);
            cloudCommitmentScopeDTO.setCloudFamilyReferenceApiDTO(cloudFamilyReferenceApiDTO);
        }
        return cloudCommitmentScopeDTO;
    }
}
