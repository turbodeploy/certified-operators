package com.vmturbo.topology.processor.targets;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.AccountDefEntryConstants;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
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
     * TargetSpecAttributeExtractor is singleton, and create a instance of a library utility to
     * verify whether a String represents a Fully Qualified Domain Name.
     */
    private DomainValidator domainValidator;

    /**
     * Create a new instance of this {@link TargetSpecAttributeExtractor}.
     *
     * @param probeStore the value of this injected probeStore
     */
    public TargetSpecAttributeExtractor(@Nonnull ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.domainValidator = DomainValidator.getInstance();
    }

    @Override
    public IdentityMatchingAttributes extractAttributes(@Nonnull final TargetSpec targetSpecItem) {
        final SimpleMatchingAttributes.Builder simpleMatchingAttributes = SimpleMatchingAttributes.newBuilder();
        final Optional<ProbeInfo> probe = probeStore.getProbe(targetSpecItem.getProbeId());
        if (probe.isPresent()) {
            final ProbeInfo targetRelatedProbe = probe.get();
            final Set<String> identifierFields = new HashSet<>(targetRelatedProbe.getTargetIdentifierFieldList());
            for (AccountValue av : targetSpecItem.getAccountValueList()) {
                String accountValueKey = av.getKey();
                if (identifierFields.contains(accountValueKey)) {
                    String accountValueStringValue = av.getStringValue();
                    // Try to convert FQDN to IP address when identifying field contains "address".
                    if (accountValueKey.contains(AccountDefEntryConstants.ADDRESS_FIELD)
                            && isValidDnsOrIpAddress(accountValueStringValue)) {
                        try {
                            accountValueStringValue = InetAddress.getByName(accountValueStringValue).getHostAddress();
                        } catch (UnknownHostException e) {
                            logger.warn(String.format("Identifying field {} for target {} is not a valid" +
                                    " domain name.", accountValueKey, accountValueStringValue));
                        }
                    }
                    simpleMatchingAttributes.addAttribute(accountValueKey, accountValueStringValue);
                }
            }
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

    /**
     * Check to see if a given string is a valid DNS address, IPV4 address, or IPV6 address.
     * The valid DNS address must be fully qualified, i.e. end in a TLD
     *
     * @param address the string to be tested
     * @return true iff the given string is a valid DNS address, an IPV4 address, or IPV6 address
     */
    boolean isValidDnsOrIpAddress(final String address) {
        return domainValidator.isValid(address) || InetAddressUtils.isIPv4Address(address) ||
                InetAddressUtils.isIPv6Address(address);
    }

}
