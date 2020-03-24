package com.vmturbo.mediation.udt;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.ISupplyChainAwareProbe;

/**
 * A probe that creates a topology from groups define by a user.
 */
public class UserDefinedTopologyProbe implements IDiscoveryProbe<UserDefinedTopologyProbeAccount>,
        ISupplyChainAwareProbe<UserDefinedTopologyProbeAccount> {

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        return new UserDefinedTopologySupplyChain().getSupplyChainDefinition();
    }

    @Nonnull
    @Override
    public Class<UserDefinedTopologyProbeAccount> getAccountDefinitionClass() {
        return UserDefinedTopologyProbeAccount.class;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull UserDefinedTopologyProbeAccount account) {
        return ValidationResponse.newBuilder().build();
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull UserDefinedTopologyProbeAccount account) {
        return DiscoveryResponse.newBuilder().build();
    }
}
