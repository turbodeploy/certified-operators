package com.vmturbo.mediation.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * CloudDiscoveryConverter for VirtualApplication. It adds vApp to be owned by BusinessAccount.
 */
public class VirtualApplicationConverter implements IEntityConverter {

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        converter.ownedByBusinessAccount(entity.getId());
        return true;
    }
}
