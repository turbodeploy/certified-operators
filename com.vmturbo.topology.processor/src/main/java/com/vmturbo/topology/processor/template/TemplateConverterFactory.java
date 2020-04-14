package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Generate a list of TopologyEntityDTO from a list of template ids. it will call TemplateService to get
 * {@link Template} and them convert them to {@link TopologyEntityDTO}.
 */
public class TemplateConverterFactory {

    private static final Logger logger = LogManager.getLogger();

    private final TemplateServiceBlockingStub templateService;

    private final IdentityProvider identityProvider;

    private final Map<Integer, TopologyEntityConstructor> templateConverterMap = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, new VirtualMachineEntityConstructor(),
            EntityType.PHYSICAL_MACHINE_VALUE, new PhysicalMachineEntityConstructor(),
            EntityType.HCI_PHYSICAL_MACHINE_VALUE, new HCIPhysicalMachineEntityConstructor(),
            EntityType.STORAGE_VALUE, new StorageEntityConstructor());

    // map used for creating reservation entities from template.
    private final Map<Integer, TopologyEntityConstructor> reservationTemplateConvertMap = ImmutableMap
            .of(EntityType.VIRTUAL_MACHINE_VALUE, new VirtualMachineEntityConstructor(true));

    public TemplateConverterFactory(@Nonnull TemplateServiceBlockingStub templateService,
                                    @Nonnull final IdentityProvider identityProvider) {
        this.templateService = Objects.requireNonNull(templateService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Convert a list of template Ids to a list of TopologyEntityDTO. It assumed all input template
     * Ids are valid. And right now, it only support Virtual Machine, Physical Machine, Storage
     * and HCI Host templates.
     *
     * @param templateAdditions map key is template id, value is the number of template need to add.
     * @param templateToReplacedEntity map key is template id, value is a list of replaced topology entity.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @return set of {@link TopologyEntityDTO}.
     */
    @Nonnull
    public Stream<TopologyEntityDTO.Builder> generateTopologyEntityFromTemplates(
            @Nonnull final Map<Long, Long> templateAdditions,
            @Nonnull Multimap<Long, Long> templateToReplacedEntity,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        Set<Long> templateIds = Sets.union(templateAdditions.keySet(),
                templateToReplacedEntity.keySet());

        Collection<Template> templates;
        try {
            templates = getTemplatesByIds(templateIds);
        } catch (TemplatesNotFoundException e) {
            logger.error("Error getting templates", e);
            return Stream.empty();
        }

        List<TopologyEntityDTO.Builder> result = new ArrayList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (Template template : templates) {
            try {
                long additionCount = templateAdditions.getOrDefault(template.getId(), 0L);
                Collection<TopologyEntityDTO.Builder> additionTemplates = generateEntityByTemplateAddition(
                        template, topology, additionCount, false);
                result.addAll(additionTemplates);

                Collection<Long> replacedEntityOids = templateToReplacedEntity
                        .get(template.getId());
                Collection<TopologyEntityDTO.Builder> replacedTemplates = generateEntityByTemplateReplaced(
                        template, topology, replacedEntityOids);
                result.addAll(replacedTemplates);
            } catch (TopologyEntityConstructorException e) {
                logger.error("Error generating topology entities from template "
                        + template.getTemplateInfo().getName(), e);
            }
        }

        stopwatch.stop();
        logger.info("Generating entities from templates took {} ms.",
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return result.stream();
    }

    /**
     * Generate reservation entities from templates. For reservation entity, it
     * only can add templates , there are no replace templates.
     *
     * @param templateAdditions map key is template id, value is the number of
     *            template need to add.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When
     *            performing a replace, entities related to the entity being
     *            replaced may be updated to fix up relationships to point to
     *            the new entity along with the old entity.
     * @return set of {@link TopologyEntityDTO} which newly created.
     * @throws TopologyEntityConstructorException error constructing topology
     *             entities
     */
    @Nonnull
    public Stream<TopologyEntityDTO.Builder> generateReservationEntityFromTemplates(
            @Nonnull final Map<Long, Long> templateAdditions,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology)
            throws TopologyEntityConstructorException {
        Collection<Template> templates = getTemplatesByIds(templateAdditions.keySet());
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        for (Template template : templates) {
            long additionCount = templateAdditions.getOrDefault(template.getId(), 0L);
            result.addAll(
                    generateEntityByTemplateAddition(template, topology, additionCount, true));
        }

        return result.stream();
    }

    /**
     * Get templates from template rpc service by input a set of template ids.
     * If there is any template missing, it will throw
     * {@link TemplatesNotFoundException}.
     *
     * @param templateIds a set of {@link Template} ids.
     * @return stream of templates.
     * @throws TemplatesNotFoundException template not found
     */
    @Nonnull
    private Collection<Template> getTemplatesByIds(@Nonnull Set<Long> templateIds)
            throws TemplatesNotFoundException {
            final List<Template> templates = TemplateProtoUtil.flattenGetResponse(
                templateService.getTemplates(GetTemplatesRequest.newBuilder()
                    .setFilter(TemplatesFilter.newBuilder()
                        .addAllTemplateIds(templateIds))
                    .build()))
                .map(SingleTemplateResponse::getTemplate)
                .collect(Collectors.toList());
            final int templatesFoundSize = templates.size();
            if (templatesFoundSize != templateIds.size()) {
                throw new TemplatesNotFoundException(templateIds.size(), templatesFoundSize);
            }

            return templates;
    }

    @Nonnull
    private Collection<TopologyEntityDTO.Builder> generateEntityByTemplateAddition(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology, final long additionCount,
            final boolean isReservation) throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        for (int i = 0; i < additionCount; i++) {
            result.addAll(generateTopologyEntityByType(template, topology, Optional.empty(),
                    isReservation, false));
        }

        return result;
    }

    @Nonnull
    private Collection<TopologyEntityDTO.Builder> generateEntityByTemplateReplaced(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Collection<Long> replacedEntityOids)
            throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        for (Long entityOid : replacedEntityOids) {
            TopologyEntity.Builder entity = topology.get(entityOid);

            if (entity == null) {
                logger.error("Error retrieving entity id {} from topology", entityOid);
                continue;
            }

            // Set shopTogether to true for all consumers of an entity to be
            // replaced.
            // VMs are the only entity types which do shop together. So we
            // filter for VMs here.
            entity.getConsumers().stream()
                    .filter(c -> c.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    .forEach(consumer -> consumer.getTopologyEntityDtoBuilder()
                            .getAnalysisSettingsBuilder().setShopTogether(true));

            result.addAll(generateTopologyEntityByType(template, topology, Optional.of(entity),
                    false, true));
        }

        return result;
    }

    @Nonnull
    private Collection<TopologyEntityDTO.Builder> generateTopologyEntityByType(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            Optional<TopologyEntity.Builder> originalTopologyEntity, final boolean isReservation,
            boolean isReplaced) throws TopologyEntityConstructorException {
        final int templateEntityType = template.getTemplateInfo().getEntityType();
        final Map<Integer, TopologyEntityConstructor> converterMap = isReservation
                ? reservationTemplateConvertMap
                : templateConverterMap;
        if (!converterMap.containsKey(templateEntityType)) {
            throw new NotImplementedException(templateEntityType + " template is not supported.");
        }

        return converterMap.get(templateEntityType).createTopologyEntityFromTemplate(template,
                topology, originalTopologyEntity, isReplaced, identityProvider);
    }
}