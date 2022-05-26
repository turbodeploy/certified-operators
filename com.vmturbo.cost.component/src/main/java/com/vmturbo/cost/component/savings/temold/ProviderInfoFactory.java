package com.vmturbo.cost.component.savings.temold;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Factory class for getting the correct type of ProviderInfo object.
 */
public class ProviderInfoFactory {

    /**
     * A dummy provider info when the provider is unknown.
     */
    private static final ProviderInfo UNKNOWN_PROVIDER = new UnknownProviderInfo();

    private static Map<Integer, BiFunction<CloudTopology<TopologyEntityDTO>, TopologyEntityDTO, ProviderInfo>>
            entityTypeToProviderInfo = ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, VirtualMachineProviderInfo::new,
                    EntityType.VIRTUAL_VOLUME_VALUE, VolumeProviderInfo::new,
                    EntityType.DATABASE_VALUE, DatabaseProviderInfo::new,
                    EntityType.DATABASE_SERVER_VALUE, DatabaseServerProviderInfo::new
            );

    /**
     * Hidden constructor.
     */
    private ProviderInfoFactory() {
    }

    /**
     * Get the provider object for the corresponding discovered topology entity. The provider is
     * retrieved from the cloud topology.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     * @return provider info
     */
    @Nonnull
    public static ProviderInfo getDiscoveredProviderInfo(
            CloudTopology<TopologyEntityDTO> cloudTopology, TopologyEntityDTO discoveredEntity) {
        return entityTypeToProviderInfo.getOrDefault(discoveredEntity.getEntityType(),
                UnknownProviderInfo::new).apply(cloudTopology, discoveredEntity);
    }

    /**
     * Get a dummy ProviderInfo object if the provider is not known.
     *
     * @return a dummy providerInfo object
     */
    public static ProviderInfo getUnknownProvider() {
        return UNKNOWN_PROVIDER;
    }

    /**
     * A dummy ProviderInfo object for an unknown provider.
     */
    static class UnknownProviderInfo extends BaseProviderInfo {
        UnknownProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
                TopologyEntityDTO discoveredEntity) {
            this();
        }

        UnknownProviderInfo() {
            super(0L, new HashMap<>(), EntityType.UNKNOWN_VALUE);
        }
    }
}
