package com.vmturbo.mediation.udt;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntry;
import com.vmturbo.platform.sdk.probe.TargetOperationException;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;

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

    /**
     * Creates a mock object of {@link ProbeInfo} for the specified probe type and probe identifier.
     *
     * @param probeId probe identifier
     * @param type probe type
     * @return Returns a mock object of {@link ProbeInfo}.
     */
    public static ProbeInfo mockProbeInfo(long probeId, SDKProbeType type) {
        ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);

        Mockito.when(probeInfo.getId()).thenReturn(probeId);
        Mockito.when(probeInfo.getType()).thenReturn(type.getProbeType());

        return probeInfo;
    }

    /**
     * Creates a set of {@link TargetInfo} with listed target identifiers and
     * for the specified probe identifier.
     *
     * @param probeId probe identifier
     * @param targetIds target identifiers
     * @return Returns a set of {@link TargetInfo}.
     */
    public static Set<TargetInfo> createMockedTargets(long probeId, long...targetIds) {
        Set<TargetInfo> targets = new HashSet<>();
        for (int i = 0; i < targetIds.length; i++) {
            TargetInfo target = Mockito.mock(TargetInfo.class);
            Mockito.when(target.getProbeId()).thenReturn(probeId);
            Mockito.when(target.getId()).thenReturn(targetIds[i]);
            targets.add(target);
        }
        return targets;
    }
}
