package com.vmturbo.mediation.conversion.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * CloudDiscoveryConverter for BusinessAccount. For AWS: set up "owns" between master account & sub account
 */
public class BusinessAccountConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public BusinessAccountConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // sub account owned by master account
        if (probeType == SDKProbeType.AWS) {
            converter.ownedByBusinessAccount(entity.getId());
            // do not set displayName for sub account target, since its displayName can not be
            // obtained due to API access restriction, and it was set as id in original probe.
            // clearing displayName field will ensure that correct displayName is set in the
            // stitching process
            if (converter.isSubAccountTarget()) {
                entity.clearDisplayName();
            }

            // clear the dataDiscovered field in BusinessAccountData if it is false, so that
            // dataDiscovered is set correctly during stitching process
            if (!entity.getBusinessAccountData().getDataDiscovered()) {
                entity.getBusinessAccountDataBuilder().clearDataDiscovered();
            }
        }
        return true;
    }
}
