package com.vmturbo.mediation.azure.pricing.resolver;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;

/**
 * A simple implementation of AzureMeterDescriptor.
 */
public class SimpleMeterDescriptor implements AzureMeterDescriptor {
    private MeterType type;
    private String subType;
    private List<String> skus;
    private List<String> regions;
    private String redundancy;
    private Integer vCoreCount;

    /**
     * Construct a meter descriptor.
     *
     * @param type the type of pricing described
     * @param subType an optional subtype
     * @param skus a list of skus for which the meter applies
     * @param regions a list of regions in which the meter applies
     * @param redundancy an optional redundancy indicator, e.g. Single AZ, Multi AZ.
     * @param vCoreCount vCore count, applies to only DC series provisioned vCore db Meter IDs.
     */
    public SimpleMeterDescriptor(@Nonnull MeterType type, @Nullable String subType,
            @Nonnull List<String> skus, @Nonnull List<String> regions, @Nullable String redundancy,
            @Nullable Integer vCoreCount) {
        this.type = type;
        this.subType = subType;
        this.skus = skus;
        this.regions = regions;
        this.redundancy = redundancy;
        this.vCoreCount = vCoreCount;
    }

    @Nonnull
    public MeterType getType() {
        return type;
    }

    @Nullable
    public String getSubType() {
        return subType;
    }

    @Nonnull
    public List<String> getSkus() {
        return skus;
    }

    @Nonnull
    public List<String> getRegions() {
        return regions;
    }

    @Nullable
    @Override
    public String getRedundancy() {
        return redundancy;
    }

    @Nullable
    @Override
    public Integer getVCoreCount() {
        return vCoreCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof AzureMeterDescriptor)) {
            return false;
        }

        AzureMeterDescriptor that = (AzureMeterDescriptor)o;

        return new EqualsBuilder().append(getType(), that.getType()).append(getSubType(),
            that.getSubType()).append(getSkus(), that.getSkus()).append(getRegions(),
            that.getRegions()).append(getRedundancy(), that.getRedundancy()).append(vCoreCount,
            that.getVCoreCount()).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(type)
            .append(subType)
            .append(skus)
            .append(regions)
            .append(redundancy)
            .append(vCoreCount)
            .toHashCode();
    }

    @Override
    public String toString() {
        return "SimpleMeterDescriptor{" + "type=" + type + ", subType='" + subType + '\''
                + ", skus=" + skus + ", regions=" + regions + ", redundancy='" + redundancy + '\''
                + ", vCoreCount=" + vCoreCount + '}';
    }
}
