package com.vmturbo.mediation.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
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
        }
        return true;
    }
}
