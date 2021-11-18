package com.vmturbo.repository.dto.cloud.commitment;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;

/**
 * Class that encapsulates the {@link FamilyRestricted}.
 */
@JsonInclude(Include.NON_EMPTY)
public class FamilyRestrictedRepoDTO {

    private String instanceFamily;

    /**
     * Constructor.
     *
     * @param familyRestricted the {@link FamilyRestricted}
     */
    public FamilyRestrictedRepoDTO(@Nonnull final FamilyRestricted familyRestricted) {
        if (familyRestricted.hasInstanceFamily()) {
            this.instanceFamily = familyRestricted.getInstanceFamily();
        }
    }

    /**
     * Creates an instance of {@link FamilyRestricted}.
     *
     * @return the {@link FamilyRestricted}
     */
    @Nonnull
    public FamilyRestricted createFamilyRestricted() {
        final FamilyRestricted.Builder familyRestricted = FamilyRestricted.newBuilder();
        if (this.instanceFamily != null) {
            familyRestricted.setInstanceFamily(this.instanceFamily);
        }
        return familyRestricted.build();
    }

    public String getInstanceFamily() {
        return instanceFamily;
    }

    public void setInstanceFamily(final String instanceFamily) {
        this.instanceFamily = instanceFamily;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FamilyRestrictedRepoDTO that = (FamilyRestrictedRepoDTO)o;
        return Objects.equals(instanceFamily, that.instanceFamily);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceFamily);
    }

    @Override
    public String toString() {
        return FamilyRestrictedRepoDTO.class.getSimpleName() + "{instanceFamily='" + instanceFamily
                + '\'' + '}';
    }
}
