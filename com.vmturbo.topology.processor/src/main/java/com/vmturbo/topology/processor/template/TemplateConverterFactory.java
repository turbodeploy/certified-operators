package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.PlanScenarioOriginImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor.TemplateActionType;

/**
 * Generate a list of TopologyEntityDTO from a list of template ids. it will call TemplateService to get
 * {@link Template} and them convert them to {@link TopologyEntityDTO}.
 */
public class TemplateConverterFactory {

    private static final Logger logger = LogManager.getLogger();

    private final TemplateServiceBlockingStub templateService;
    private final IdentityProvider identityProvider;
    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final Map<Integer, ITopologyEntityConstructor> templateConverterMap;

    /**
     * Object for matching templates to the appropriate TopologyEntityDTO. Each type of template
     * has unique characteristics handled by it's appropriate ITopologyEntityConstructor.
     *
     * @param templateService the service used to retrieve template information.
     * @param identityProvider manages re-use and creation of oids.
     * @param settingPolicyService the service used to get setting policy info from.
     * @param cpuCapacityService the service used to estimate the scaling factor of a cpu model.
     */
    public TemplateConverterFactory(@Nonnull TemplateServiceBlockingStub templateService,
            @Nonnull final IdentityProvider identityProvider,
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull CpuCapacityServiceBlockingStub cpuCapacityService) {
        this.templateService = Objects.requireNonNull(templateService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.settingPolicyService = Objects.requireNonNull(settingPolicyService);

        templateConverterMap = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, new VirtualMachineEntityConstructor(
                Objects.requireNonNull(cpuCapacityService)),
            EntityType.PHYSICAL_MACHINE_VALUE, new PhysicalMachineEntityConstructor(),
            // TODO OM-59961 Do not handle the HCI Host as a regular host
            EntityType.HCI_PHYSICAL_MACHINE_VALUE, new PhysicalMachineEntityConstructor(),
            EntityType.STORAGE_VALUE, new StorageEntityConstructor());
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
     * @param topologyId topology id.
     * @return set of {@link TopologyEntityDTO}.
     */
    @Nonnull
    public Stream<TopologyEntityImpl> generateTopologyEntityFromTemplates(
            @Nonnull final Map<Long, Long> templateAdditions,
            @Nonnull Multimap<Long, Long> templateToReplacedEntity,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology, long topologyId) {
        Set<Long> templateIds = Sets.union(templateAdditions.keySet(),
                templateToReplacedEntity.keySet());

        Collection<Template> templates;
        try {
            templates = getTemplatesByIds(templateIds);
        } catch (TemplatesNotFoundException e) {
            logger.error("Error getting templates", e);
            return Stream.empty();
        }

        List<TopologyEntityImpl> result = new ArrayList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (Template template : templates) {
            try {
                long additionCount = templateAdditions.getOrDefault(template.getId(), 0L);
                Collection<TopologyEntityImpl> additionTemplates = generateEntityByTemplateAddition(
                        template, topology, additionCount, topologyId);
                result.addAll(additionTemplates);

                Collection<Long> replacedEntityOids = templateToReplacedEntity
                        .get(template.getId());
                Collection<TopologyEntityImpl> replacedTemplates = generateEntityByTemplateReplaced(
                        template, topology, replacedEntityOids, topologyId);
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
    private Collection<TopologyEntityImpl> generateEntityByTemplateAddition(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology, final long additionCount, long topologyId) throws TopologyEntityConstructorException {
        List<TopologyEntityImpl> result = new ArrayList<>();

        for (int i = 0; i < additionCount; i++) {
            result.add(generateTopologyEntityByType(template, topology, null,
                    TemplateActionType.CLONE, "(Clone " + i + ")", topologyId));
        }

        return result;
    }

    @Nonnull
    private Collection<TopologyEntityImpl> generateEntityByTemplateReplaced(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Collection<Long> replacedEntityOids, long topologyId)
            throws TopologyEntityConstructorException {

        List<TopologyEntity.Builder> entitiesToReplace = getEntitiesById(topology,
                replacedEntityOids);
        setShopTogether(entitiesToReplace);

        if (template.getTemplateInfo().getEntityType() == EntityType.HCI_PHYSICAL_MACHINE_VALUE) {
            return new HCIPhysicalMachineEntityConstructor(template, topology, identityProvider,
                    settingPolicyService).replaceEntitiesFromTemplate(entitiesToReplace);
        } else {
            List<TopologyEntityImpl> result = new ArrayList<>();

            for (TopologyEntity.Builder entity : entitiesToReplace) {
                result.add(generateTopologyEntityByType(template, topology,
                        entity.getTopologyEntityImpl(), TemplateActionType.REPLACE, null, topologyId));
            }

            return result;
        }
    }

    /**
     * Set shopTogether to true for all consumers of an entity to be replaced.
     * VMs are the only entity types which do shop together. So we filter for
     * VMs here.
     *
     * @param entitiesToReplace entities to replace
     */
    private static void setShopTogether(@Nonnull List<TopologyEntity.Builder> entitiesToReplace) {
        for (TopologyEntity.Builder entity : entitiesToReplace) {
            for (TopologyEntity consumer : entity.getConsumers()) {
                if (consumer.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
                    continue;
                }

                consumer.getTopologyEntityImpl().getOrCreateAnalysisSettings()
                        .setShopTogether(true);
            }
        }
    }

    private static List<TopologyEntity.Builder> getEntitiesById(
            @Nonnull Map<Long, TopologyEntity.Builder> topology, @Nonnull Collection<Long> oids)
            throws TopologyEntityConstructorException {

        List<TopologyEntity.Builder> result = new ArrayList<>();

        for (Long entityOid : oids) {
            TopologyEntity.Builder entity = topology.get(entityOid);

            if (entity == null) {
                throw new TopologyEntityConstructorException(
                        "Error retrieving entity id " + entityOid + " from topology");
            }

            result.add(entity);
        }

        return result;
    }

    @Nonnull
    private TopologyEntityImpl generateTopologyEntityByType(@Nonnull Template template,
            @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntityImpl originalTopologyEntity,
            @Nonnull TemplateActionType actionType, @Nullable String nameSuffix, long topologyId)
            throws TopologyEntityConstructorException {
        final int templateEntityType = template.getTemplateInfo().getEntityType();
        final Map<Integer, ITopologyEntityConstructor> converterMap =
                templateConverterMap;
        if (!converterMap.containsKey(templateEntityType)) {
            throw new TopologyEntityConstructorException(
                    "Template type " + templateEntityType + "  is not supported.");
        }

        final TopologyEntityImpl entityFromTemplate = converterMap.get(templateEntityType)
                .createTopologyEntityFromTemplate(template, topology, originalTopologyEntity,
                        actionType, identityProvider, nameSuffix);

        // entity added in plan are marked with a plan origin
        final PlanScenarioOriginImpl planOrigin =
                new PlanScenarioOriginImpl().setPlanId(topologyId);
        if (originalTopologyEntity != null) {
            planOrigin.setOriginalEntityId(originalTopologyEntity.getOid());
        }
        entityFromTemplate.setOrigin(
                new OriginImpl().setPlanScenarioOrigin(planOrigin));
        return entityFromTemplate;
    }
}