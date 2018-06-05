package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.supplychain.errors.AmbiguousBaseTemplateException;
import com.vmturbo.topology.processor.supplychain.errors.NoDiscoveryOriginException;
import com.vmturbo.topology.processor.supplychain.errors.NoProbeException;
import com.vmturbo.topology.processor.supplychain.errors.NoSupplyChainDefinitionException;
import com.vmturbo.topology.processor.supplychain.errors.NoTargetException;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This class stores supply chain definitions of probes.  It can be used to fetch a collection of relevant
 * {@link TemplateDTO} objects, given an entity type id.  The supply chain validation will check this entity
 * against all these templates.  This is the job of the method
 * {@link SupplyChainDefinitions#retrieveSupplyChainTemplates(TopologyEntity)}.
 *
 * The collection of templates is determined by the targets that discover the entity.  With each target is
 * associated a probe and with that probe is associated a single template.  In the collection returned,
 * we include a subset of these templates, in particular:
 * - the template of type {@link TemplateType#BASE} with highest priority.
 * - all templates of type {@link TemplateType#EXTENSION}.
 *
 * The class maintains an internal cache of templates and fetches these definitions lazily from a probe and
 * a target store.
 */
@ThreadSafe
public class SupplyChainDefinitions {
    private final Logger logger = LogManager.getLogger();

    /**
     * Table to cache the supply chain relationship between probe, entity category, and supply chain
     * template.
     * The row value is probe id, column is entity category id, and the value is the corresponding
     * {{@link TemplateDTO}} object.
     */
    private final Table<Long, Integer, TemplateDTO> templateDefinitions = HashBasedTable.create();

    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    /**
     * Create the object given a probe store and a target store to look for the definitions.
     *
     * @param probeStore the probe store.
     * @param targetStore the target store.
     */
    public SupplyChainDefinitions(@Nonnull ProbeStore probeStore, @Nonnull TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    @Nonnull
    private TemplateDTO getTemplate(long probeId, @Nonnull TopologyEntity entity)
          throws SupplyChainValidationException {
        // look for the template in the cache
        TemplateDTO template = templateDefinitions.get(probeId, entity.getEntityType());
        if (template == null) {
            // not found.  get it from the stores and put it in the cache
            final ProbeInfo probeInfo = probeStore.getProbe(probeId)
                    .orElseThrow(() -> new NoProbeException(entity, probeId));
            template = probeInfo.getSupplyChainDefinitionSetList().stream()
                    .filter(templateDTO ->
                        templateDTO.getTemplateClass().getNumber() == entity.getEntityType())
                    .findFirst()
                    .orElseThrow(() -> new NoSupplyChainDefinitionException(entity, probeInfo));
            templateDefinitions.put(probeId, entity.getEntityType(), template);
        }
        logger.trace(
                "Obtained template from probe {} with id {} and entity type {}",
                () -> probeStore.getProbe(probeId).map(ProbeInfo::getProbeType).orElse("null"),
                () -> probeId, entity::getEntityType);
        return template;
    }

    /**
     * Retrieves the instances of {@link TemplateDTO} that should be used to verify a specific entity.
     *
     * @param entity entity for which we want to retrieve the templates.
     * @return collection of templates against which the entity must be validated
     * @throws SupplyChainValidationException retrieval failed: This is a fatal error.
     *          The entity cannot be supply-chain validated.
     */
    public Set<TemplateDTO> retrieveSupplyChainTemplates(
          @Nonnull final TopologyEntity entity) throws SupplyChainValidationException {
        final List<SupplyChainValidationException> errorList = new ArrayList<>();
        final TopologyEntityDTOOrBuilder entityDTOOrBuilder =
            Objects.requireNonNull(entity).getTopologyEntityDtoBuilder();
        logger.trace("Retrieving supply chain templates for {}", entity::getDisplayName);

        // ensure there are targets discovering this entity and fetch them
        if (!entityDTOOrBuilder.hasOrigin() || !entityDTOOrBuilder.getOrigin().hasDiscoveryOrigin()) {
            throw new NoDiscoveryOriginException(entity);
        }
        final Collection<Long> discoveringTargetIds =
            entityDTOOrBuilder.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList();
        logger.trace(
            "Entity {} has {} origins", entity::getDisplayName, discoveringTargetIds::size);

        // this variable will hold one of the max-priority base templates found
        TemplateDTO maxPriorityBaseTemplate = null;

        // this variable will hold the number of max-priority base templates found
        int maxPriorityBaseTemplatesFound = 0;

        // the set of templates to return
        final Set<TemplateDTO> templates = new HashSet<>();

        // this variable will hold the set of probe ids found
        // this will be used only to prevent adding a template twice
        // (if a probe has been found before, it will not be processed again)
        final Set<Long> probeIdsFound = new HashSet<>();

        // Loop all targets which discovered the entity and find the max priority base template and all
        // extension templates
        for (final long targetId : discoveringTargetIds) {
            // get probe id
            final long probeId =
                targetStore.getTarget(targetId).orElseThrow(() -> new NoTargetException(entity, targetId)).
                getProbeId();
            logger.trace(
                "Entity {} is discovered by probe with id {}",
                entity::getDisplayName, () -> probeId);
            if (!probeIdsFound.add(probeId)) {
                logger.trace("Probe {} is already considered: skipping", probeId);
                continue;
            }

            // find the related template
            final TemplateDTO template = getTemplate(probeId, entity);

            // examine the template type
            if (template.getTemplateType() == TemplateType.BASE) {
                // base template
                logger.trace("Base template with priority {}", template::getTemplatePriority);
                if (maxPriorityBaseTemplate == null ||
                        template.getTemplatePriority() > maxPriorityBaseTemplate.getTemplatePriority()) {
                    // found a new max-priority
                    maxPriorityBaseTemplate = template;
                    maxPriorityBaseTemplatesFound = 1;
                } else if (template.getTemplatePriority() == maxPriorityBaseTemplate.getTemplatePriority()) {
                    // found a new template with the max priority
                    maxPriorityBaseTemplatesFound++;
                }
            } else {
                // extension template, just add it in the collection
                logger.trace("Extension template");
                templates.add(template);
            }
        }
        logger.trace("Number of max priority base templates is {}", maxPriorityBaseTemplatesFound);
        if (maxPriorityBaseTemplatesFound != 1) {
            logger.warn(
                new AmbiguousBaseTemplateException(entity, maxPriorityBaseTemplatesFound).toString());
        }

        // add the base template with highest priority
        if (maxPriorityBaseTemplate != null) {
            templates.add(maxPriorityBaseTemplate);
        }

        logger.trace("Total number of templates returned is {}", templates::size);
        return templates;
    }
}
