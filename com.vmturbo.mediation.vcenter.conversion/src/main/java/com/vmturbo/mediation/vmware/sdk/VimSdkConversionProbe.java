package com.vmturbo.mediation.vmware.sdk;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Add an EnabledStorageBrowsing flag to the list of account values for VCenter and if storage
 * browsing is disabled, strip out derived storage browsing target from discovery response.
 */
public class VimSdkConversionProbe extends VimSdkProbe {
    private final Logger logger = LogManager.getLogger();

    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final VimAccount accountValues) throws InterruptedException {
        return new AddVirtualVolumeDiscoveryConverter(
            getRawDiscoveryResponse(accountValues), false).convert();
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

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        // create supply chain node for virtual volume to avoid warnings in TP
        return ConverterUtils.addBasicTemplateDTO(super.getSupplyChainDefinition(),
            EntityType.VIRTUAL_VOLUME);
    }
}
