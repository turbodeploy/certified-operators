package com.vmturbo.repository.dto.cloud.commitment;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ServiceRestricted;

/**
 * Class that encapsulates the {@link ServiceRestricted}.
 */
@JsonInclude(Include.NON_EMPTY)
public class ServiceRestrictedRepoDTO {

    /**
     * Constructor.
     *
     * @param serviceRestricted the {@link ServiceRestricted}
     */
    public ServiceRestrictedRepoDTO(@Nonnull final ServiceRestricted serviceRestricted) {
    }

    /**
     * Creates an instance of {@link ServiceRestricted}.
     *
     * @return the {@link ServiceRestricted}
     */
    @Nonnull
    public ServiceRestricted createServiceRestricted() {
        return ServiceRestricted.getDefaultInstance();
    }

    @Override
    public String toString() {
        return ServiceRestrictedRepoDTO.class.getSimpleName() + "{}";
    }
}
