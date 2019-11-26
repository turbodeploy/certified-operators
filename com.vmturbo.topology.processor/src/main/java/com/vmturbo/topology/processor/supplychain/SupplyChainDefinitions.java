package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import com.vmturbo.topology.processor.supplychain.errors.DuplicateTemplateFailure;
import com.vmturbo.topology.processor.supplychain.errors.NoProbeFailure;
import com.vmturbo.topology.processor.supplychain.errors.NoSupplyChainDefinitionFailure;
import com.vmturbo.topology.processor.supplychain.errors.NoTargetFailure;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;
import com.vmturbo.topology.processor.targets.Target;
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
 */
@ThreadSafe
public class SupplyChainDefinitions {
    private final Logger logger = LogManager.getLogger();

    /**
     * In this table, we record all supply chain templates per probe id and entity category.
     * The row value is probe id, column is entity category id, and the value is the corresponding
     * {{@link TemplateDTO}} object.
     */
    private final Table<Long, Integer, TemplateDTO> templateDefinitions = HashBasedTable.create();

    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final List<SupplyChainValidationFailure> validationErrors = new ArrayList<>();

    /**
     * Creates the object given a probe store and a target store to look for the definitions.
     * Populates the internal template definitions table.
     *
     * @param probeStore the probe store.
     * @param targetStore the target store.
     */
    public SupplyChainDefinitions(@Nonnull ProbeStore probeStore, @Nonnull TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);

        for (Map.Entry<Long, ProbeInfo> entry : probeStore.getProbes().entrySet()) {
            final long probeId = entry.getKey();
            for (TemplateDTO template : entry.getValue().getSupplyChainDefinitionSetList()) {
                final int entryCategory = template.getTemplateClass().ordinal();
                if (templateDefinitions.contains(probeId, entry)) {
                    validationErrors.add(new DuplicateTemplateFailure(probeId, entryCategory));
                }
                templateDefinitions.put(probeId, entryCategory, template);
            }
        }
    }

    /**
     * Return a list of errors that occurred during the population of the table of templates.
     *
     * @return list of errors.
     */
    public List<SupplyChainValidationFailure> getValidationErrors() {
        return Collections.unmodifiableList(validationErrors);
    }

    /**
     * Retrieves the instances of {@link TemplateDTO} that should be used to verify a specific entity.
     *
     * @param entity entity for which we want to retrieve the templates.
     * @return collection of templates against which the entity must be validated
     */
    public Collection<TemplateDTO> retrieveSupplyChainTemplates(@Nonnull final TopologyEntity entity) {
        final TopologyEntityDTOOrBuilder entityDTOOrBuilder =
            Objects.requireNonNull(entity).getTopologyEntityDtoBuilder();
        logger.trace("Retrieving supply chain templates for {}", entity::getDisplayName);

        final Collection<Long> discoveringTargetIds =
            entityDTOOrBuilder.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet();
        logger.trace(
            "Entity {} has {} origins", entity::getDisplayName, discoveringTargetIds::size);

        // this variable will hold one of the max-priority base templates found
        TemplateDTO maxPriorityBaseTemplate = null;

        // this variable will hold the number of max-priority base templates found
        int maxPriorityBaseTemplatesFound = 0;

        // the set of templates to return
        final Collection<TemplateDTO> templates = new ArrayList<>();

        // this variable will hold the set of probe ids found
        // this will be used only to prevent adding a template twice
        // (if a probe has been found before, it will not be processed again)
        final Set<Long> probeIdsFound = new HashSet<>();

        // Loop all targets which discovered the entity and find the max priority base template and all
        // extension templates
        for (final long targetId : discoveringTargetIds) {
            // get target and probe id
            final Optional<Target> target = targetStore.getTarget(targetId);
            if (!target.isPresent()) {
                validationErrors.add(new NoTargetFailure(entity, targetId));
                continue;
            }
            final long probeId = target.get().getProbeId();
            logger.trace(
                "Entity {} is discovered by probe with id {}",
                entity::getDisplayName, () -> probeId);
            if (!probeIdsFound.add(probeId)) {
                logger.trace("Probe {} is already considered: skipping", probeId);
                continue;
            }

            // find the related template
            final TemplateDTO template = templateDefinitions.get(probeId, entity.getEntityType());
            if (template == null) {
                // template not found: check if probe id is missing
                final Optional<ProbeInfo> probeInfo = probeStore.getProbe(probeId);
                if (probeInfo.isPresent()) {
                    // probe exists, but has no template for this entity type
                    validationErrors.add(new NoSupplyChainDefinitionFailure(entity, probeInfo.get()));
                } else {
                    validationErrors.add(new NoProbeFailure(entity, probeId));
                }
                continue;
            }

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
                "The entity {} has {} max priority supply chain validation templates.  One is expected.",
                entity.getDisplayName(), maxPriorityBaseTemplatesFound);
        }

        // add the base template with highest priority
        if (maxPriorityBaseTemplate != null) {
            templates.add(maxPriorityBaseTemplate);
        }

        logger.trace("Total number of templates returned is {}", templates::size);
        return templates;
    }
}
