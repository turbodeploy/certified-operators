package com.vmturbo.topology.processor.targets;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * The {@link TargetSpec} attribute extractor which is mainly used for filtering duplicate targets. We
 * compare values in target identifier fields in TargetSpec with other existing targets to see if they
 * are all matched, then we can regard the target as exist.
 */
public class TargetSpecAttributeExtractor implements AttributeExtractor<TargetSpec> {

    public static final String PROBE_TYPE = "probeType";

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    /**
     * Create a new instance of this {@link TargetSpecAttributeExtractor}.
     *
     * @param probeStore the value of this injected probeStore
     */
    public TargetSpecAttributeExtractor(@Nonnull ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    @Override
    public IdentityMatchingAttributes extractAttributes(@Nonnull final TargetSpec targetSpecItem) {
        final SimpleMatchingAttributes.Builder simpleMatchingAttributes = SimpleMatchingAttributes.newBuilder();
        final Optional<ProbeInfo> probe = probeStore.getProbe(targetSpecItem.getProbeId());
        if (probe.isPresent()) {
            final ProbeInfo targetRelatedProbe = probe.get();
            final Set<String> identifierFields = new HashSet<>(targetRelatedProbe.getTargetIdentifierFieldList());
            targetSpecItem.getAccountValueList().forEach(av -> {
                if (identifierFields.contains(av.getKey())) {
                    simpleMatchingAttributes.addAttribute(av.getKey(), av.getStringValue());
                }
            });
            // Targets are uniquely identified by their id plus the probe type that discovered
            // them. We can have multiple instances of the same probe type, but they can't
            // discover the same target twice
            simpleMatchingAttributes.addAttribute(PROBE_TYPE, targetRelatedProbe.getProbeType());
        } else {
            logger.error("Extracting target spec attributes failed! No related probe found with id {}.",
                    targetSpecItem.getProbeId());
        }
        return simpleMatchingAttributes.build();
    }

}
