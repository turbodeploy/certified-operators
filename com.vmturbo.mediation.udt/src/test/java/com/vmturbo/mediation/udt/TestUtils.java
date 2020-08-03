package com.vmturbo.mediation.udt;

import java.io.Serializable;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntry;
import com.vmturbo.platform.sdk.probe.TargetOperationException;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Utility class for UDT test.
 */
public class TestUtils {

    /**
     * Utility classes should not have a public or default constructor.
     */
    private TestUtils() {

    }

    /**
     * The method creates DTO: TopologyEntityDTO.
     *
     * @param id   - ID of entity.
     * @param name - name of entity.
     * @param type - type of entity.
     * @return TopologyEntityDTO.
     */
    public static TopologyEntityDTO createTopologyDto(long id, String name, EntityType type) {
        return TopologyEntityDTO
                .newBuilder()
                .setOid(id)
                .setDisplayName(name)
                .setEntityType(type.getNumber())
                .build();
    }

    /**
     * The method creates 'dummy' probe context.
     *
     * @return IProbeContext.
     */
    public static IProbeContext getIProbeContext() {
        return new IProbeContext() {
            @Nonnull
            @Override
            public IPropertyProvider getPropertyProvider() {
                return new IPropertyProvider() {
                    @Nonnull
                    @Override
                    public <T> T getProperty(@Nonnull IProbePropertySpec<T> iProbePropertySpec) {
                        return iProbePropertySpec.getDefaultValue();
                    }
                };
            }

            @Override
            public <T extends Serializable> IProbeDataStoreEntry<T> getTargetFeatureCategoryData(@Nonnull String s) throws TargetOperationException {
                return null;
            }
        };
    }
}
