package com.vmturbo.mediation.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * CloudDiscoveryConverter for LoadBalancer. It adds LB to be owned by BusinessAccount.
 */
public class LoadBalancerConverter implements IEntityConverter {

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        converter.ownedByBusinessAccount(entity.getId());
        return true;
    }
}
