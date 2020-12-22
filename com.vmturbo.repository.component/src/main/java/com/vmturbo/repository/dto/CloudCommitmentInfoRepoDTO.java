package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ServiceRestricted;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Class that encapsulates the CloudCommitmentInfo from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class CloudCommitmentInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private static final Logger logger = LogManager.getLogger();

    private ServiceRestricted serviceRestricted;

    private FamilyRestricted familyRestricted;

    private CurrencyAmount spend;

    private Long numCoupons;

    private PaymentOption paymentOption;

    private Long termInMiliSeconds;

    private Long startTimeInMiliSeconds;

    private Long expirationTimeInMiliSeconds;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
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

        switch (cloudCommitmentInfo.getScopeCase()) {
            case SERVICE_RESTRICTED:
                setServiceRestricted(cloudCommitmentInfo.getServiceRestricted());
                break;
            case FAMILY_RESTRICTED:
                setFamilyRestricted(cloudCommitmentInfo.getFamilyRestricted());
                break;
            default:
                logger.error("CloudCommitmentDataInfoRepo: No scope found on Cloud Commitment Data {}", cloudCommitmentInfo.toString());
                break;
        }
        switch (cloudCommitmentInfo.getCommitmentCase()) {
            case SPEND:
                setSpend(cloudCommitmentInfo.getSpend());
                break;
            case NUMBER_COUPONS:
                setNumCoupons(cloudCommitmentInfo.getNumberCoupons());
                break;
            default:
                logger.error("CloudCommitmentDataInfoRepo: No commitment found on Cloud Commitment Data {}", cloudCommitmentInfo.toString());
        }
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final CloudCommitmentInfo.Builder cloudCommitmentDataBuilder = CloudCommitmentInfo.newBuilder();
        if (getExpirationTimeInMiliSeconds() != null) {
            cloudCommitmentDataBuilder.setExpirationTimeMilliseconds(getExpirationTimeInMiliSeconds());
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
            cloudCommitmentDataBuilder.setFamilyRestricted(getFamilyRestricted());
        }
        if (getServiceRestricted() != null) {
            cloudCommitmentDataBuilder.setServiceRestricted(getServiceRestricted());
        }
        if (getSpend() != null) {
            cloudCommitmentDataBuilder.setSpend(getSpend());
        }
        if (getNumCoupons() != null) {
            cloudCommitmentDataBuilder.setNumberCoupons(getNumCoupons());
        }

        return TypeSpecificInfo.newBuilder().setCloudCommitmentData(cloudCommitmentDataBuilder.build())
                .build();
    }

    public Long getTermInMiliSeconds() {
        return termInMiliSeconds;
    }

    public void setTermInMiliSeconds(Long termInMiliSeconds) {
        this.termInMiliSeconds = termInMiliSeconds;
    }

    public Long getStartTimeInMiliSeconds() {
        return startTimeInMiliSeconds;
    }

    public void setStartTimeInMiliSeconds(Long startTimeInMiliSeconds) {
        this.startTimeInMiliSeconds = startTimeInMiliSeconds;
    }

    public Long getExpirationTimeInMiliSeconds() {
        return expirationTimeInMiliSeconds;
    }

    public void setExpirationTimeInMiliSeconds(Long expirationTimeInMiliSeconds) {
        this.expirationTimeInMiliSeconds = expirationTimeInMiliSeconds;
    }

    public PaymentOption getPaymentOption() {
        return paymentOption;
    }

    public void setPaymentOption(PaymentOption paymentOption) {
        this.paymentOption = paymentOption;
    }

    public void setServiceRestricted(ServiceRestricted serviceRestricted) {
        this.serviceRestricted = serviceRestricted;
    }

    public ServiceRestricted getServiceRestricted() {
        return serviceRestricted;
    }

    public void setFamilyRestricted(FamilyRestricted familyRestricted) {
        this.familyRestricted = familyRestricted;
    }

    public FamilyRestricted getFamilyRestricted() {
        return familyRestricted;
    }

    public void setSpend(CurrencyAmount spend) {
        this.spend = spend;
    }

    public CurrencyAmount getSpend() {
        return spend;
    }

    public void setNumCoupons(Long numCoupons) {
        this.numCoupons = numCoupons;
    }

    public Long getNumCoupons() {
        return numCoupons;
    }
}
