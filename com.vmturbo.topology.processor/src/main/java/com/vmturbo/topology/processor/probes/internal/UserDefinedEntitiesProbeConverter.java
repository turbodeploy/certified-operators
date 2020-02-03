package com.vmturbo.topology.processor.probes.internal;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * A converter of entities for 'UserDefinedEntities' probe.
 */
public class UserDefinedEntitiesProbeConverter {

    /**
     * Makes a converting from groups into response of the probe.
     *
     * @param groups - collection of groups.
     * @return {@link DiscoveryResponse} instance.
     */
    @Nonnull
    public DiscoveryResponse convertToResponse(@Nonnull Collection<GroupDTO> groups) {
        // TODO: implement converting.
        return DiscoveryResponse.newBuilder().build();
    }

}
