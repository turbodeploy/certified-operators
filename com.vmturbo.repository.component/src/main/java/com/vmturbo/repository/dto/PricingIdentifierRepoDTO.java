package com.vmturbo.repository.dto;

import java.util.Objects;

import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;

/**
 * Class for holding pricing identifier info for a business account.  Currently used by Azure to
 * specify offer id and enrollment number fields which are used for determining the price table to
 * use.
 */
public class PricingIdentifierRepoDTO {

    private PricingIdentifierName identifierName;
    private String identifierValue;

    public PricingIdentifierRepoDTO() {
        identifierName = null;
        identifierValue = null;
    }

    public PricingIdentifierRepoDTO(PricingIdentifierName name, String value) {
        identifierName = name;
        identifierValue = value;
    }

    /**
     * Get the identifier name.
     * @return identifierName field value.
     */
    public PricingIdentifierName getIdentifierName() {
        return identifierName;
    }

    /**
     * Get identifier value.
     * @return identifierValue field value.
     */
    public String getIdentifierValue() {
        return identifierValue;
    }

    /**
     * Set the value of the identifierName field.
     *
     * @param identifierName {@link PricingIdentifierName} giving the new value of identifierName.
     */
    public void setIdentifierName(final PricingIdentifierName identifierName) {
        this.identifierName = identifierName;
    }

    /**
     * Set the value of the identifierValue field.
     *
     * @param identifierValue {@link String} giving the new value of identifierValue.
     */
    public void setIdentifierValue(final String identifierValue) {
        this.identifierValue = identifierValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final PricingIdentifierRepoDTO that = (PricingIdentifierRepoDTO) o;

        return Objects.equals(identifierName, that.identifierName)
            && Objects.equals(identifierValue, that.identifierValue);

    }

    @Override
    public int hashCode() {
        return Objects.hash(identifierName, identifierValue);
    }

    @Override
    public String toString() {
        return "PricingIdentifierRepoDTO{" +
            "identifierName=" + identifierName +
            ", identifierValue='" + identifierValue + '\'' +
            '}';
    }
}
