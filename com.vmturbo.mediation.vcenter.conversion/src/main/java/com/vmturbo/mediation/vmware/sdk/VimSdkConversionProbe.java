package com.vmturbo.mediation.vmware.sdk;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.cloud.util.ConverterUtils;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Add an EnabledStorageBrowsing flag to the list of account values for VCenter and if storage
 * browsing is disabled, strip out derived storage browsing target from discovery response.
 */
public class VimSdkConversionProbe extends VimSdkProbe {
    private final Logger logger = LogManager.getLogger();

    @Override
    public Class getAccountDefinitionClass() {
        return VimAccountWithStorageBrowsingFlag.class;
    }

    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final VimAccount accountValues) throws InterruptedException {
        DiscoveryResponse rawDiscoveryResponse =  getRawDiscoveryResponse(accountValues);
        if (accountValues instanceof VimAccountWithStorageBrowsingFlag) {
            if (!((VimAccountWithStorageBrowsingFlag) accountValues).isStorageBrowsingEnabled()) {
                return ConverterUtils.removeDerivedTargets(rawDiscoveryResponse,
                    SDKProbeType.VC_STORAGE_BROWSE);
            }
        } else {
            logger.error("Unexpected class of AccountValue in discoverTarget {}",
                accountValues.getClass());
        }
        return rawDiscoveryResponse;
    }

    /**
     * Return the discovery response from the VimSdkProbe.  Needed in a separate method to
     * facilitate testing.
     *
     * @param accountValues {@link VimAccount} values to use for discovery.
     * @return {@link DiscoveryResponse} from the VC probe.
     * @throws InterruptedException
     */
    protected DiscoveryResponse getRawDiscoveryResponse(@Nonnull final VimAccount accountValues)
        throws InterruptedException {
        return super.discoverTarget(accountValues);
    }
}
