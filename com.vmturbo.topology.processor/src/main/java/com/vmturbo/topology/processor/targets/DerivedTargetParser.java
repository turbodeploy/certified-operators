package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
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
            final String targetName = derivedTargetDTO.getAccountValueList().stream()
                        .filter(accountValue -> accountValue.getKey().equalsIgnoreCase(
                                PredefinedAccountDefinition.Address.name()))
                        .map(Discovery.AccountValue::getStringValue)
                        .findFirst()
                        .orElse(derivedTargetDTO.toString());
            try {
                final long probeId = probeStore.getProbeIdForType(derivedTargetDTO.getProbeType())
                        .orElseThrow(() -> new InvalidTargetException(
                                String.format("No probe type %s found.", derivedTargetDTO.getProbeType())));
                final TargetSpec targetSpec = parseDerivedTargetSpec(probeId, parentTargetId,
                                derivedTargetDTO);
                // If the target is dependent on the parent target, then we create it as derived target, or
                // we should create the target as normal way as the case for VCD creating VC targets.
                if (targetSpec.hasParentId()) {
                    derivedTargetSpecs.add(targetSpec);
                } else {
                    try {
                        targetStore.createTarget(targetSpec);
                    } catch (DuplicateTargetException e) {
                        logger.debug("Derived target {} already exists, parsing will be skipped.",
                                targetName);
                    }
                }
            } catch (InvalidTargetException | IdentityStoreException e) {
                logger.error("Parse derived target {} failed. {}", targetName, e);
            }
        });
        try {
            targetStore.createOrUpdateDerivedTargets(derivedTargetSpecs);
        } catch (IdentityStoreException e) {
            logger.error("Error when fetching derived target identifiers and OIDs, derived targets creation "
                    + "or update failed. {}", e);
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
        targetSpec.setProbeId(probeId);
        targetSpec.setIsHidden(derivedTargetDTO.getHidden());
        if (derivedTargetDTO.hasDependent() && derivedTargetDTO.getDependent()) {
            targetSpec.setParentId(parentTargetId);
        }
        return targetSpec.build();
    }
}
