package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.sdk.common.util.ProbeCategory.CUSTOM;
import static com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import static com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import static com.vmturbo.topology.processor.probes.internal.UserDefinedEntitiesProbe.UDE_PROBE_TYPE;
import static com.vmturbo.topology.processor.probes.internal.UserDefinedEntitiesProbeAccount.UDE_DISPLAY_NAME;
import static com.vmturbo.topology.processor.probes.internal.UserDefinedEntitiesProbeAccount.UDE_FIELD_NAME;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.mediation.common.EntityIdentityMetadataLoader;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;

/**
 * Class responsible for defining parameters of the 'UserDefinedEntities' Probe.
 */
public class UserDefinedEntitiesProbeDefinition implements InternalProbeDefinition {

    private static final long TWO_MINUTES_SEC = TimeUnit.MINUTES.toSeconds(2);
    private static final Logger LOGGER = LogManager.getLogger();

    private final ProbeInfo probeInfo;
    private final InternalProbeTransportSubstitute transport;

    /**
     * Constructor.
     *
     * @param retrieval - a supplier for groups and group members.
     */
    @ParametersAreNonnullByDefault
    public UserDefinedEntitiesProbeDefinition(@Nonnull UserDefinedEntitiesProbeRetrieval retrieval) {
        final UserDefinedEntitiesProbe internalProbe
                = new UserDefinedEntitiesProbe(retrieval);
        probeInfo = createProbeInfo(internalProbe.getSupplyChainDefinition());
        transport = createTransport(internalProbe );
    }

    @Override
    @Nonnull
    public String getProbeType() {
        return UDE_PROBE_TYPE;
    }

    /**
     * Creates a definition of the probe 'UserDefinedEntities'.
     *
     * @return {@link ProbeInfo} instance.
     */
    @Nonnull
    public ProbeInfo getProbeInfo() {
        return probeInfo;
    }

    @Override
    @Nonnull
    public ITransport<MediationServerMessage, MediationClientMessage> getTransport() {
        return transport;
    }

    /**
     * Creates a definition of a target for the probe 'UserDefinedEntities'.
     *
     * @param probeId - ID of an internal probe.
     * @return {@link TargetSpec} instance.
     */
    @Nonnull
    public TargetSpec getProbeTarget(long probeId) {
        return TargetSpec.newBuilder()
                .setProbeId(probeId)
                .setIsHidden(true)
                .addAccountValue(AccountValue.newBuilder()
                        .setKey(UDE_FIELD_NAME)
                        .setStringValue(UDE_FIELD_NAME)
                        .build())
                .build();
    }

    @Nonnull
    private ProbeInfo createProbeInfo(@Nonnull Set<TemplateDTO> supplyChainDefinition) {
        return ProbeInfo.newBuilder()
                .setProbeType(UDE_PROBE_TYPE)
                .setUiProbeCategory(UDE_PROBE_TYPE)
                .setProbeCategory(CUSTOM.getCategory())
                .setCreationMode(CreationMode.INTERNAL)
                .addAllSupplyChainDefinitionSet(supplyChainDefinition)
                .setFullRediscoveryIntervalSeconds((int)TWO_MINUTES_SEC)
                .addAccountDefinition(Discovery.AccountDefEntry.newBuilder()
                        .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                                .setName(UDE_FIELD_NAME)
                                .setDisplayName(UDE_DISPLAY_NAME)
                                .setDescription("")
                                .setIsSecret(false)
                                .setPrimitiveValue(PrimitiveValue.STRING)
                                .build())
                        .build())
                .addTargetIdentifierField(UDE_FIELD_NAME)
                .addAllEntityMetadata(getMetadata())
                .build();
    }

    @Nonnull
    private InternalProbeTransportSubstitute createTransport(@Nonnull UserDefinedEntitiesProbe probe) {
        return new InternalProbeTransportSubstitute(probe);
    }

    @Nonnull
    private List<EntityIdentityMetadata> getMetadata() {
        try {
            InputStream inputStream = getIdentityMetadataInputStream();
            if (inputStream != null) {
                return EntityIdentityMetadataLoader.loadConfiguration(inputStream);
            }
            LOGGER.warn("Input stream [identity-metadata.yml] is NULL.");
        } catch (ProbeConfigurationLoadException e) {
            LOGGER.error(e);
        }
        return Collections.emptyList();
    }

    @Nullable
    private InputStream getIdentityMetadataInputStream() {
        return this.getClass().getResourceAsStream("/identity-metadata.yml");
    }
}
