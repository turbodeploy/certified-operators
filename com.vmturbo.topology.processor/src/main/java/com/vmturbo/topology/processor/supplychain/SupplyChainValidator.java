package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This class implements supply chain validation.
 */
public class SupplyChainValidator {
    private final Logger logger = LogManager.getLogger();

    private final SupplyChainDefinitions supplyChainDefinitions;

    /**
     * Initialize a supply chain validator, giving access to a probe store and a target store.
     *
     * @param probeStore the probe store.
     * @param targetStore the target store.
     */
    public SupplyChainValidator(@Nonnull ProbeStore probeStore, @Nonnull TargetStore targetStore) {
        supplyChainDefinitions = new SupplyChainDefinitions(probeStore, targetStore);
    }

    /**
     * Validates a stream of entities from a topology graph, according to the supply
     * chain definitions given by the probes.
     *
     * @param entities the stream of entities to validate.
     * @return list of all supply chain errors encountered.
     */
    @Nonnull
    public List<SupplyChainValidationException> validateTopologyEntities(
          @Nonnull final Stream<TopologyEntity> entities) {
        logger.info("Supply Chain validation");

        final List<SupplyChainValidationException> validationErrors = new ArrayList<>();

        // iterate through discovered entities
        entities.filter(TopologyEntity::hasDiscoveryOrigin).forEach(entity -> {
            try {
                logger.trace("Supply chain validation for entity {} begins", entity::getDisplayName);
                final Set<TemplateDTO> templates =
                    supplyChainDefinitions.retrieveSupplyChainTemplates(entity);
                logger.trace("Retrieval of templates for entity {} was successful", entity::getDisplayName);
                validationErrors.addAll(
                    SupplyChainEntityValidator.verify(templates, entity));
            } catch (SupplyChainValidationException e) {
                logger.trace(
                    "Fatal error during validation for entity {}: {}",
                    entity::getDisplayName, e::toString);
                validationErrors.add(e);
            }
        });

        // log all errors
        validationErrors.forEach(e -> logger.error(e.toString()));
        logger.info("{} supply chain validation errors have been detected.", validationErrors::size);

        logger.info("Supply Chain validation finished");

        return Collections.unmodifiableList(validationErrors);
    }
}
