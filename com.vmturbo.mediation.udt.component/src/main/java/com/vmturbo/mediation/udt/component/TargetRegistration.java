package com.vmturbo.mediation.udt.component;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.UDT;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Class is responsible for a registration of UDT probe target in the TopologyProcessor.
 * It runs one time on the probe component starting up.
 */
class TargetRegistration {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String UDT_TARGET_FIELD_NAME = "targetName";
    private static final String UDT_TARGET_FIELD_VALUE = "User-defined entities";

    private final TopologyProcessor topologyProcessor;

    TargetRegistration(TopologyProcessor topologyProcessor) {
        this.topologyProcessor = topologyProcessor;
    }

    /**
     * The method checks if a target is already created. If not, it creates new one.
     *
     * @throws CommunicationException in case of any connection error.
     * @throws TopologyProcessorException if the TopologyProcessor is not available.
     */
    void checkTargetRegistration() throws CommunicationException, TopologyProcessorException {
        final ProbeInfo udtProbe = findUdtProbe();
        if (udtProbe == null) {
            throw new IllegalStateException("UDT probe not found");
        }
        if (!isTargetRegistered(udtProbe.getId())) {
            registerTarget(udtProbe.getId());
        }
    }

    private void registerTarget(long probeId) throws TopologyProcessorException, CommunicationException {
        long targetId = topologyProcessor.addTarget(probeId, getTargetData());
        LOGGER.info("UDT Target is registered: " + targetId);
    }

    private boolean isTargetRegistered(long probeId) throws CommunicationException {
        final TargetInfo target = findUdtTarget(probeId);
        if (target != null) {
            LOGGER.info("UDT Target found: " + target.getId());
        }
        return target != null;
    }

    @Nullable
    private ProbeInfo findUdtProbe() throws CommunicationException {
        return topologyProcessor.getAllProbes()
                .stream().filter(probe -> probe.getType().equals(UDT.getProbeType()))
                .findFirst().orElse(null);
    }

    @Nullable
    private TargetInfo findUdtTarget(long probeId) throws CommunicationException {
        return topologyProcessor.getAllTargets().stream()
                .filter(t -> t.getProbeId() == probeId).findFirst().orElse(null);
    }

    @Nonnull
    private TargetData getTargetData() {
        return new TargetData() {
            @Nonnull
            @Override
            public Set<AccountValue> getAccountData() {
                return Sets.newHashSet(accountValue);
            }

            @Nonnull
            @Override
            public Optional<String> getCommunicationBindingChannel() {
                return Optional.empty();
            }
        };
    }

    private static final AccountValue accountValue = new AccountValue() {
        @Nonnull
        @Override
        public String getName() {
            return UDT_TARGET_FIELD_NAME;
        }

        @Nullable
        @Override
        public String getStringValue() {
            return UDT_TARGET_FIELD_VALUE;
        }

        @Nullable
        @Override
        public List<List<String>> getGroupScopeProperties() {
            return Collections.emptyList();
        }
    };

}
