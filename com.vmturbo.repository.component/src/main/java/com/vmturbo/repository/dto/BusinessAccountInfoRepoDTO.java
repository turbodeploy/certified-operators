package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.PricingIdentifier;

/**
 * Class that encapsulates the business account data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class BusinessAccountInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private Boolean hasAssociatedTarget;
    private String accountId;
    private List<PricingIdentifierRepoDTO> pricingIdentifiers;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasBusinessAccount()) {
            return;
        }
        BusinessAccountInfo businessAccountInfo = typeSpecificInfo.getBusinessAccount();
        setHasAssociatedTarget(businessAccountInfo.getHasAssociatedTarget());
        final List<PricingIdentifierRepoDTO> pricingIds = Lists.newArrayList();
        setAccountId(businessAccountInfo.getAccountId());
        businessAccountInfo.getPricingIdentifiersList()
            .forEach(pricingIdentifier ->
                pricingIds
                    .add(new PricingIdentifierRepoDTO(pricingIdentifier.getIdentifierName(),
                        pricingIdentifier.getIdentifierValue())));
        setPricingIdentifiers(pricingIds);
        serviceEntityRepoDTO.setBusinessAccountInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final BusinessAccountInfo.Builder businessAccountInfo = BusinessAccountInfo.newBuilder();
        if (hasAssociatedTarget != null) {
            businessAccountInfo.setHasAssociatedTarget(hasAssociatedTarget);
        }
        if (accountId != null) {
            businessAccountInfo.setAccountId(accountId);
        }
        if (pricingIdentifiers != null) {
            pricingIdentifiers.forEach(pricingId -> businessAccountInfo
                .addPricingIdentifiers(PricingIdentifier.newBuilder()
                    .setIdentifierName(pricingId.getIdentifierName())
                    .setIdentifierValue(pricingId.getIdentifierValue()).build()));
        }
        return TypeSpecificInfo.newBuilder()
                .setBusinessAccount(businessAccountInfo)
                .build();
    }

    /**
     * Sets value for hasAssociatedTarget indicating whether this business account was discovered
     * by a dedicated target (true) or only by the master (false).
     *
     * @param hasAssociatedTarget {@link Boolean} indicating new value for hasAssociatedTarget.
     */
    public void setHasAssociatedTarget(final Boolean hasAssociatedTarget) {
        this.hasAssociatedTarget = hasAssociatedTarget;
    }

    /**
     * Gets value for hasAssociatedTarget indicating whether this business account was discovered
     * by a dedicated target (true) or only by the master (false).
     *
     * @return {@link Boolean} indicating the current value of hasAssociatedTarget.
     */
    public Boolean getHasAssociatedTarget() {
        return hasAssociatedTarget;
    }

    /**
     * Get the current value of accountId which is the identifier used to identify this account
     * by the provider.
     *
     * @return {@link String} giving the accountId.
     */
    public String getAccountId() {
        return accountId;
    }

    /**
     * Set the current value of accountId which is the identifier used to identify this account
     * by the provider.
     *
     * @param accountId the value of the accountId.
     */
    public void setAccountId(final String accountId) {
        this.accountId = accountId;
    }

    /**
     * Get the list of pricing identifiers for this business account.  These hold the attributes
     * that determine the pricing scheme for entities under this account.
     *
     * @return A list of {@link PricingIdentifierRepoDTO} with the pricing attributes and their
     * values.
     */
    public List<PricingIdentifierRepoDTO> getPricingIdentifiers() {
        return pricingIdentifiers;
    }

    /**
     * Set the list of pricing identifiers for this business account.  These hold the attributes
     * that determine the pricing scheme for entities under this account.
     *
     * @param pricingIdentifiers the list of {@link PricingIdentifierRepoDTO}.
     */
    public void setPricingIdentifiers(final List<PricingIdentifierRepoDTO> pricingIdentifiers) {
        this.pricingIdentifiers = pricingIdentifiers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BusinessAccountInfoRepoDTO that = (BusinessAccountInfoRepoDTO) o;

        return Objects.equals(hasAssociatedTarget, that.getHasAssociatedTarget()) &&
            Objects.equals(accountId, that.getAccountId()) &&
            Objects.equals(pricingIdentifiers, that.getPricingIdentifiers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasAssociatedTarget, accountId, pricingIdentifiers);
    }

    @Override
    public String toString() {
        return "BusinessAccountInfoRepoDTO{" +
                "hasAssociatedTarget=" + hasAssociatedTarget +
                ", accountId=" + accountId +
                ", pricingIdentifiers=" + pricingIdentifiers +
                '}';
    }
}
