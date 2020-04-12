package com.vmturbo.mediation.conversion.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * CloudDiscoveryConverter for Application. It adds App to be owned by BusinessAccount.
 */
public class ApplicationComponentConverter implements IEntityConverter {

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        converter.ownedByBusinessAccount(entity.getId());
        return true;
    }
}
