package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This class implements supply chain validation.
 */
public class SupplyChainValidator {
    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    /**
     * Initialize a supply chain validator, giving access to a probe store and a target store.
     *
     * @param probeStore  the probe store.
     * @param targetStore the target store.
     */
    public SupplyChainValidator(@Nonnull ProbeStore probeStore, @Nonnull TargetStore targetStore) {
        this.probeStore = probeStore;
        this.targetStore = targetStore;
    }

    /**
     * Validates a stream of entities from a topology graph, according to the supply
     * chain definitions given by the probes.
     *
     * @param entities the stream of entities to validate.
     * @return list of all supply chain errors encountered.
     */
    @Nonnull
    public List<SupplyChainValidationFailure> validateTopologyEntities(
        @Nonnull final Stream<TopologyEntity> entities) {
        // renew supply chain definitions
        // this happens because new probes might have registered since the last time
        // the supply chain validator was invoked
        final SupplyChainDefinitions supplyChainDefinitions =
            new SupplyChainDefinitions(probeStore, targetStore);

        // include errors that happened during the creation of the validation definitions
        // in the final collection of errors
        final List<SupplyChainValidationFailure> validationErrors =
            new ArrayList<>(supplyChainDefinitions.getValidationErrors());

        // iterate through discovered entities
        entities.filter(TopologyEntity::hasDiscoveryOrigin).forEach(entity -> {
            logger.trace("Supply chain validation for entity {} begins",
                entity::getDisplayName);
            final Collection<TemplateDTO> templates =
                supplyChainDefinitions.retrieveSupplyChainTemplates(entity);
            validationErrors.addAll(SupplyChainEntityValidator.verify(templates, entity));
        });

        if (!validationErrors.isEmpty()) {
            boolean debug = logger.isDebugEnabled();
            if (debug) {
                // log all errors if debug logging is enabled
                validationErrors.forEach(validationError -> logger.error(validationError.toString()));
            }
            // otherwise just log consolidated error messages - one per subclass of
            // SupplyChainValidationFailure that appears in the validation errors. If DEBUG is
            // enabled, these are summarizing ERROR logs already produced, so they go out at INFO
            // level. Otherwise, they're announcing errors, so they go at ERROR level.
            validationErrors.stream()
                .collect(Collectors.groupingBy(SupplyChainValidationFailure::getClass,
                    Collectors.counting()))
                .forEach((type, count) -> logger.log(debug ? Level.INFO : Level.ERROR,
                    "Supply chain validation errors of type {}: {}", type, count));
            // Total is always a summary, so it goes out at INFO level
            logger.info("Supply chain validation errors total: {}",
                validationErrors.size());
        } else {
            logger.info("No supply chain validation errors detected.");
        }

        return Collections.unmodifiableList(validationErrors);
    }
}
