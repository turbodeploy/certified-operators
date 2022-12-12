package com.vmturbo.mediation.azure.pricing.enums;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.platform.sdk.common.CloudCostDTO;

/**
 * Alternative for CloudCostDTO.DeploymentType with an additional field.
 */
public enum DeploymentType {

    /**
     * Represents no deployment available.
     */
    NONE,

    /**
     * Represents the multi-AZ  deployment.
     */
    MULTI_AZ("MULTI_AZ", CloudCostDTO.DeploymentType.MULTI_AZ),

    /**
     * Represents the single-AZ  deployment.
     */
    SINGLE_AZ("SINGLE_AZ", CloudCostDTO.DeploymentType.SINGLE_AZ);

    private Pattern pattern;
    private CloudCostDTO.DeploymentType dtoEnum;

    DeploymentType() {

    }

    /**
     * Creates {@link com.vmturbo.mediation.azure.pricing.enums.DeploymentType} instance.
     *
     * @param rawPattern represents possible raw values which might come.
     * @param dtoEnum represents the {@link CloudCostDTO.DeploymentType} equivalent of this
     *         enum.
     */
    DeploymentType(String rawPattern, CloudCostDTO.DeploymentType dtoEnum) {
        this.pattern = Pattern.compile(rawPattern, Pattern.CASE_INSENSITIVE);
        this.dtoEnum = dtoEnum;
    }

    /**
     * Returns the {@link CloudCostDTO.DeploymentType} equivalent of this enum.
     *
     * @return the {@link CloudCostDTO.DeploymentType} equivalent of this enum.
     */
    public CloudCostDTO.DeploymentType getDtoEnum() {
        return dtoEnum;
    }

    /**
     * Parses the input string and finds the deployment type represented by it based on the a
     * pattern.
     *
     * @param rawValue the input string.
     * @return the deployment type otherwise empty.
     */
    @Nonnull
    public static Optional<com.vmturbo.mediation.azure.pricing.enums.DeploymentType> from(
            @Nullable String rawValue) {
        if (StringUtils.isBlank(rawValue)) {
            return Optional.of(DeploymentType.NONE);
        }
        return Stream.of(com.vmturbo.mediation.azure.pricing.enums.DeploymentType.values()).filter(
                dt -> dt.pattern != null && dt.pattern.matcher(rawValue).matches()).findAny();
    }
}
