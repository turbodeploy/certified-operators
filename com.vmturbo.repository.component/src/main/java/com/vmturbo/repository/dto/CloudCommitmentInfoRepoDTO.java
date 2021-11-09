package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ProviderType;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.repository.dto.cloud.commitment.CommittedCommoditiesBoughtRepoDTO;
import com.vmturbo.repository.dto.cloud.commitment.FamilyRestrictedRepoDTO;
import com.vmturbo.repository.dto.cloud.commitment.ServiceRestrictedRepoDTO;

/**
 * Class that encapsulates the CloudCommitmentInfo from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class CloudCommitmentInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private static final Logger logger = LogManager.getLogger();

    private ServiceRestrictedRepoDTO serviceRestricted;

    private FamilyRestrictedRepoDTO familyRestricted;

    private CurrencyAmountRepoDTO spend;

    private Long numCoupons;

    private CommittedCommoditiesBoughtRepoDTO commoditiesBought;

    private PaymentOption paymentOption;

    private Long termInMiliSeconds;

    private Long startTimeInMiliSeconds;

    private Long expirationTimeInMiliSeconds;

    private CloudCommitmentStatus cloudCommitmentStatus;

    private CloudCommitmentScope cloudCommitmentScope;

    private ProviderType providerSpecificType;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
            @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasCloudCommitmentData()) {
            return;
        }
        final CloudCommitmentInfo cloudCommitmentInfo = typeSpecificInfo.getCloudCommitmentData();
        if (cloudCommitmentInfo.hasPayment()) {
            setPaymentOption(cloudCommitmentInfo.getPayment());
        }
        if (cloudCommitmentInfo.hasTermMilliseconds()) {
            setTermInMiliSeconds(cloudCommitmentInfo.getTermMilliseconds());
        }
        if (cloudCommitmentInfo.hasStartTimeMilliseconds()) {
            setStartTimeInMiliSeconds(cloudCommitmentInfo.getStartTimeMilliseconds());
        }
        if (cloudCommitmentInfo.hasExpirationTimeMilliseconds()) {
            setExpirationTimeInMiliSeconds(cloudCommitmentInfo.getExpirationTimeMilliseconds());
        }
        if (cloudCommitmentInfo.hasCommitmentStatus()) {
            setCloudCommitmentStatus(cloudCommitmentInfo.getCommitmentStatus());
        }
        if (cloudCommitmentInfo.hasCommitmentScope()) {
            setCloudCommitmentScope(cloudCommitmentInfo.getCommitmentScope());
        }
        if (cloudCommitmentInfo.hasProviderSpecificType()) {
            setProviderSpecificType(cloudCommitmentInfo.getProviderSpecificType());
        }
        switch (cloudCommitmentInfo.getScopeCase()) {
            case SERVICE_RESTRICTED:
                setServiceRestricted(
                        new ServiceRestrictedRepoDTO(cloudCommitmentInfo.getServiceRestricted()));
                break;
            case FAMILY_RESTRICTED:
                setFamilyRestricted(
                        new FamilyRestrictedRepoDTO(cloudCommitmentInfo.getFamilyRestricted()));
                break;
            default:
                logger.error(
                        "CloudCommitmentDataInfoRepo: No scope found on Cloud Commitment Data {}",
                        cloudCommitmentInfo);
                break;
        }
        switch (cloudCommitmentInfo.getCommitmentCase()) {
            case SPEND:
                setSpend(new CurrencyAmountRepoDTO(cloudCommitmentInfo.getSpend()));
                break;
            case NUMBER_COUPONS:
                setNumCoupons(cloudCommitmentInfo.getNumberCoupons());
                break;
            case COMMODITIES_BOUGHT:
                setCommoditiesBought(new CommittedCommoditiesBoughtRepoDTO(
                        cloudCommitmentInfo.getCommoditiesBought()));
                break;
            default:
                logger.error(
                        "CloudCommitmentDataInfoRepo: No commitment found on Cloud Commitment Data {}",
                        cloudCommitmentInfo);
        }
        serviceEntityRepoDTO.setCloudCommitmentInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final Builder cloudCommitmentDataBuilder = CloudCommitmentInfo.newBuilder();
        if (getExpirationTimeInMiliSeconds() != null) {
            cloudCommitmentDataBuilder.setExpirationTimeMilliseconds(
                    getExpirationTimeInMiliSeconds());
        }
        if (getPaymentOption() != null) {
            cloudCommitmentDataBuilder.setPayment(getPaymentOption());
        }
        if (getTermInMiliSeconds() != null) {
            cloudCommitmentDataBuilder.setTermMilliseconds(getTermInMiliSeconds());
        }
        if (getStartTimeInMiliSeconds() != null) {
            cloudCommitmentDataBuilder.setStartTimeMilliseconds(getStartTimeInMiliSeconds());
        }
        if (getFamilyRestricted() != null) {
            cloudCommitmentDataBuilder.setFamilyRestricted(
                    getFamilyRestricted().createFamilyRestricted());
        }
        if (getServiceRestricted() != null) {
            cloudCommitmentDataBuilder.setServiceRestricted(
                    getServiceRestricted().createServiceRestricted());
        }
        if (getSpend() != null) {
            cloudCommitmentDataBuilder.setSpend(getSpend().createCurrencyAmount());
        }
        if (getNumCoupons() != null) {
            cloudCommitmentDataBuilder.setNumberCoupons(getNumCoupons());
        }
        if (getCommoditiesBought() != null) {
            cloudCommitmentDataBuilder.setCommoditiesBought(
                    getCommoditiesBought().createCommittedCommoditiesBought());
        }
        if (getCloudCommitmentStatus() != null) {
            cloudCommitmentDataBuilder.setCommitmentStatus(getCloudCommitmentStatus());
        }
        if (getCloudCommitmentScope() != null) {
            cloudCommitmentDataBuilder.setCommitmentScope(getCloudCommitmentScope());
        }
        if (getProviderSpecificType() != null) {
            cloudCommitmentDataBuilder.setProviderSpecificType(getProviderSpecificType());
        }

        return TypeSpecificInfo.newBuilder()
                .setCloudCommitmentData(cloudCommitmentDataBuilder)
                .build();
    }

    public Long getTermInMiliSeconds() {
        return termInMiliSeconds;
    }

    public void setTermInMiliSeconds(final Long termInMiliSeconds) {
        this.termInMiliSeconds = termInMiliSeconds;
    }

    public Long getStartTimeInMiliSeconds() {
        return startTimeInMiliSeconds;
    }

    public void setStartTimeInMiliSeconds(final Long startTimeInMiliSeconds) {
        this.startTimeInMiliSeconds = startTimeInMiliSeconds;
    }

    public Long getExpirationTimeInMiliSeconds() {
        return expirationTimeInMiliSeconds;
    }

    public void setExpirationTimeInMiliSeconds(final Long expirationTimeInMiliSeconds) {
        this.expirationTimeInMiliSeconds = expirationTimeInMiliSeconds;
    }

    public PaymentOption getPaymentOption() {
        return paymentOption;
    }

    public void setPaymentOption(final PaymentOption paymentOption) {
        this.paymentOption = paymentOption;
    }

    public void setServiceRestricted(final ServiceRestrictedRepoDTO serviceRestricted) {
        this.serviceRestricted = serviceRestricted;
    }

    public ServiceRestrictedRepoDTO getServiceRestricted() {
        return serviceRestricted;
    }

    public void setFamilyRestricted(final FamilyRestrictedRepoDTO familyRestricted) {
        this.familyRestricted = familyRestricted;
    }

    public FamilyRestrictedRepoDTO getFamilyRestricted() {
        return familyRestricted;
    }

    public void setSpend(final CurrencyAmountRepoDTO spend) {
        this.spend = spend;
    }

    public CurrencyAmountRepoDTO getSpend() {
        return spend;
    }

    public void setNumCoupons(final Long numCoupons) {
        this.numCoupons = numCoupons;
    }

    public Long getNumCoupons() {
        return numCoupons;
    }

    public CloudCommitmentStatus getCloudCommitmentStatus() {
        return cloudCommitmentStatus;
    }

    public void setCloudCommitmentStatus(final CloudCommitmentStatus cloudCommitmentStatus) {
        this.cloudCommitmentStatus = cloudCommitmentStatus;
    }

    public CloudCommitmentScope getCloudCommitmentScope() {
        return cloudCommitmentScope;
    }

    public void setCloudCommitmentScope(final CloudCommitmentScope cloudCommitmentScope) {
        this.cloudCommitmentScope = cloudCommitmentScope;
    }

    public CommittedCommoditiesBoughtRepoDTO getCommoditiesBought() {
        return commoditiesBought;
    }

    public void setCommoditiesBought(final CommittedCommoditiesBoughtRepoDTO commoditiesBought) {
        this.commoditiesBought = commoditiesBought;
    }

    public void setProviderSpecificType(final ProviderType providerSpecificType) {
        this.providerSpecificType = providerSpecificType;
    }

    public ProviderType getProviderSpecificType() {
        return providerSpecificType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CloudCommitmentInfoRepoDTO that = (CloudCommitmentInfoRepoDTO)o;
        return Objects.equals(serviceRestricted, that.serviceRestricted) && Objects.equals(
                familyRestricted, that.familyRestricted) && Objects.equals(spend, that.spend)
                && Objects.equals(numCoupons, that.numCoupons) && Objects.equals(commoditiesBought,
                that.commoditiesBought) && paymentOption == that.paymentOption && Objects.equals(
                termInMiliSeconds, that.termInMiliSeconds) && Objects.equals(startTimeInMiliSeconds,
                that.startTimeInMiliSeconds) && Objects.equals(expirationTimeInMiliSeconds,
                that.expirationTimeInMiliSeconds)
                && cloudCommitmentStatus == that.cloudCommitmentStatus
                && cloudCommitmentScope == that.cloudCommitmentScope
                && providerSpecificType == that.providerSpecificType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceRestricted, familyRestricted, spend, numCoupons,
                commoditiesBought, paymentOption, termInMiliSeconds, startTimeInMiliSeconds,
                expirationTimeInMiliSeconds, cloudCommitmentStatus, cloudCommitmentScope,
                providerSpecificType);
    }

    @Override
    public String toString() {
        return CloudCommitmentInfoRepoDTO.class.getSimpleName() + "{serviceRestricted="
                + serviceRestricted + ", familyRestricted=" + familyRestricted + ", spend=" + spend
                + ", numCoupons=" + numCoupons + ", commoditiesBought=" + commoditiesBought
                + ", paymentOption=" + paymentOption + ", termInMiliSeconds=" + termInMiliSeconds
                + ", startTimeInMiliSeconds=" + startTimeInMiliSeconds
                + ", expirationTimeInMiliSeconds=" + expirationTimeInMiliSeconds
                + ", cloudCommitmentStatus=" + cloudCommitmentStatus + ", providerSpecificType="
                + providerSpecificType + ", cloudCommitmentScope=" + cloudCommitmentScope + '}';
    }
}
