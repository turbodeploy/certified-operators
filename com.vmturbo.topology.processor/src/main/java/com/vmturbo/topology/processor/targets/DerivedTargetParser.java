package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.TargetLinkInfoDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue.PropertyValueList;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * This class is used to parse the Derived target data from the discovery response of parent target, then
 * create new Derived Target based on the returned info.
 */
@ThreadSafe
public class DerivedTargetParser {

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final TargetStore targetStore;

    public DerivedTargetParser(@Nonnull final ProbeStore probeStore, @Nonnull final TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Get the derived target display name for logs, in case of failure.
     *
     * @param probeId the probe ID
     * @param derivedTargetDTO the derived target for which we want to get the display name.
     * @return the derived target display name for logging.
     */
    private String getDerivedTargetDisplayName(long probeId, DerivedTargetSpecificationDTO derivedTargetDTO) {
        TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder();
        targetSpecBuilder.setProbeId(probeId);
        // convert Discovery.AccountValue to TopologyProcessorDTO.AccountValue
        derivedTargetDTO.getAccountValueList().forEach(accountValue -> {
            targetSpecBuilder.addAccountValue(AccountValue.newBuilder()
                    .setKey(accountValue.getKey())
                    .setStringValue(accountValue.getStringValue())
                    .build());
        });
        return Target.computeDisplayName(targetSpecBuilder.build(), probeStore);
    }

    /**
     * Instantiates all derived targets and parses the DerivedTargetSpecificationDTO that returned from
     * discovery responses.
     *
     * @param parentTargetId The id of the parent target.
     * @param derivedTargetsList The DTOs of derived targets list returned from discovery response.
     */
    public void instantiateDerivedTargets(final long parentTargetId,
            @Nonnull final List<DerivedTargetSpecificationDTO> derivedTargetsList) {
        logger.trace(derivedTargetsList.size() + " derived targets found.");
        final List<TargetSpec> derivedTargetSpecs = new ArrayList<>();
        derivedTargetsList.forEach(derivedTargetDTO -> {
            try {
                final long probeId = probeStore.getProbeIdForType(derivedTargetDTO.getProbeType())
                        .orElseThrow(() -> new InvalidTargetException(
                                String.format("No probe type %s found.", derivedTargetDTO.getProbeType())));
                String targetName = getDerivedTargetDisplayName(probeId, derivedTargetDTO);
                try {
                    final TargetSpec targetSpec = parseDerivedTargetSpec(probeId, parentTargetId,
                            derivedTargetDTO);
                    // If the target is dependent on the parent target, then we create it as derived target, or
                    // we should create the target as normal way as the case for VCD creating VC targets.
                    if (derivedTargetDTO.hasDependent() && derivedTargetDTO.getDependent()) {
                        derivedTargetSpecs.add(targetSpec);
                    } else {
                        try {
                            targetStore.createTarget(targetSpec);
                        } catch (DuplicateTargetException e) {
                            logger.debug("Derived target {} already exists, parsing will be skipped.",
                                    targetName, e);
                        }
                    }
                } catch (InvalidTargetException | IdentityStoreException | TargetNotFoundException | IdentifierConflictException e) {
                    logger.error("Parse derived target {} failed", targetName, e);
                }
            } catch (InvalidTargetException e) {
                logger.error("Probe not registered", e);
            }
        });
        try {
            targetStore.createOrUpdateDerivedTargets(derivedTargetSpecs, parentTargetId);
        } catch (IdentityStoreException e) {
            logger.error("Error when fetching derived target identifiers and OIDs, derived targets creation "
                    + "or update failed", e);
        }
    }

    private TargetSpec parseDerivedTargetSpec(final long probeId, final long parentTargetId,
            @Nonnull final DerivedTargetSpecificationDTO derivedTargetDTO) throws InvalidTargetException {
        final TargetSpec.Builder targetSpec = TargetSpec.newBuilder();
        derivedTargetDTO.getAccountValueList().forEach(av -> {
            final AccountValue.Builder accountValue = AccountValue.newBuilder();
            accountValue.setKey(av.getKey());
            accountValue.setStringValue(av.getStringValue());
            av.getGroupScopePropertyValuesList().stream().forEach(v -> {
                accountValue.addGroupScopePropertyValues(PropertyValueList.newBuilder()
                        .addAllValue(v.getValueList()).build());
            });
            targetSpec.addAccountValue(accountValue);
        });
        Optional<Target> parentTarget = targetStore.getTarget(parentTargetId);
        if (parentTarget.isPresent() && parentTarget.get().getSpec().hasCommunicationBindingChannel()) {
            targetSpec.setCommunicationBindingChannel(parentTarget.get().getSpec().getCommunicationBindingChannel());
        }
        targetSpec.setReadOnly(derivedTargetDTO.getReadonly());
        targetSpec.setProbeId(probeId);
        targetSpec.setIsHidden(derivedTargetDTO.getHidden());

        if (derivedTargetDTO.hasTargetLinkInfo()) {

            final TargetLinkInfoDTO linkInfoDTO = derivedTargetDTO.getTargetLinkInfo();

            logger.info("Processing link information for probe ID {} from parent target ID {}:\n{}",
                    probeId, parentTargetId, linkInfoDTO);


            targetSpec.putParentLinks(parentTargetId, linkInfoDTO);
        }

        return targetSpec.build();
    }
}
