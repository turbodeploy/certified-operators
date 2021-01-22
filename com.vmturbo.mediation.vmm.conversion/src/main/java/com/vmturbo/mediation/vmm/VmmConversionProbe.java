package com.vmturbo.mediation.vmm;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.mediation.vmm.sdk.VmmAccount;
import com.vmturbo.mediation.vmm.sdk.VmmProbe;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;

/**
 * The wrapper probe around original VmmProbe probe, which stands between mediation and VmmProbe.
 * It takes the topology discovered by HyperVSDKProbe and adds VirtualVolumes between VMs and
 * Storages, one VirtualVolume for each pair of vm-storage.
 */
public class VmmConversionProbe extends VmmProbe {

    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final VmmAccount accountValues) throws InterruptedException {
        return new AddVirtualVolumeDiscoveryConverter(
            getRawDiscoveryResponse(accountValues), false).convert();
    }

    /**
     * Return the discovery response from the VmmProbe.  Needed in a separate method to
     * facilitate testing.
     *
     * @param accountValues {@link VmmAccount} values to use for discovery.
     * @return {@link DiscoveryResponse} from the VMM probe.
     * @throws InterruptedException
     */
    protected DiscoveryResponse getRawDiscoveryResponse(@Nonnull final VmmAccount accountValues)
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
