package com.vmturbo.topology.processor.probes.internal;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainBuilder;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.ISupplyChainAwareProbe;

/**
 * A probe that creates a topology from groups define by a user.
 */
public class UserDefinedEntitiesProbe implements IDiscoveryProbe<UserDefinedEntitiesProbeAccount>,
        ISupplyChainAwareProbe<UserDefinedEntitiesProbeAccount> {

    private static final Logger LOGGER = LogManager.getLogger();

    // TODO: move it to 'com.vmturbo.platform.sdk.common.util.SDKProbeType'
    /**
     * The type of this probe.
     */
    public static final String UDE_PROBE_TYPE = "User Defined Entities";

    @Nonnull
    @Override
    public Class<UserDefinedEntitiesProbeAccount> getAccountDefinitionClass() {
        return UserDefinedEntitiesProbeAccount.class;
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        return new SupplyChainBuilder().configure();
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull UserDefinedEntitiesProbeAccount account)
            throws InterruptedException {
        return ValidationResponse.newBuilder().build();
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull UserDefinedEntitiesProbeAccount account)
            throws InterruptedException {
        return discoverTarget(account, null);
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull UserDefinedEntitiesProbeAccount account,
                                            @Nullable Discovery.DiscoveryContextDTO context)
            throws InterruptedException {
        final String targetName = account.getTargetName();
        try {
            final UserDefinedEntitiesProbeConverter converter = new UserDefinedEntitiesProbeConverter();
            final UserDefinedEntitiesProbeExplorer explorer = new UserDefinedEntitiesProbeExplorer();
            return converter.convertToResponse(explorer.getUserDefinedGroups());
        } catch (Exception e) {
            LOGGER.info("Internal probe {} discovery error.", targetName);
            return SDKUtil.createDiscoveryError(e.getMessage());
        }

    }

}
