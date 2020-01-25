package com.vmturbo.topology.processor.template;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.TopologyEditorException;

/**
 * Generate a list of TopologyEntityDTO from a list of template ids. it will call TemplateService to get
 * {@link Template} and them convert them to {@link TopologyEntityDTO}.
 */
public class TemplateConverterFactory {
    private final TemplateServiceBlockingStub templateService;

    private final IdentityProvider identityProvider;

    private final Map<Integer, TopologyEntityConstructor> templateConverterMap = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, new VirtualMachineEntityConstructor(),
            EntityType.PHYSICAL_MACHINE_VALUE, new PhysicalMachineEntityConstructor(),
            EntityType.STORAGE_VALUE, new StorageEntityConstructor()
    );

    // map used for creating reservation entities from template.
    private final Map<Integer, TopologyEntityConstructor> reservationTemplateConvertMap = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE,
            new VirtualMachineEntityConstructor(true)
    );

    public TemplateConverterFactory(@Nonnull TemplateServiceBlockingStub templateService,
                                    @Nonnull final IdentityProvider identityProvider) {
        this.templateService = Objects.requireNonNull(templateService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Convert a list of template Ids to a list of TopologyEntityDTO. It assumed all input template
     * Ids are valid. And right now, it only support Virtual Machine, Physical Machine and Storage
     * templates.
     *
     * @param templateAdditions map key is template id, value is the number of template need to add.
     * @param templateToReplacedEntity map key is template id, value is a list of replaced topology entity.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @return set of {@link TopologyEntityDTO}.
     */
    public Stream<TopologyEntityDTO.Builder> generateTopologyEntityFromTemplates(
            @Nonnull final Map<Long, Long> templateAdditions,
            @Nonnull Multimap<Long, Long> templateToReplacedEntity,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final Set<Long> templateIds = Sets.union(templateAdditions.keySet(), templateToReplacedEntity.keySet());
        final Stream<Template> templates = getTemplatesByIds(templateIds);
        return templates.flatMap(template -> {
            final long additionCount = templateAdditions.getOrDefault(template.getId(), 0L);
            final Collection<Long> replacedEntities =
                    templateToReplacedEntity.get(template.getId());
            final Stream<TopologyEntityDTO.Builder> additionTemplates =
                    generateEntityByTemplateAddition(template, topology, additionCount, false);
            final Stream<TopologyEntityDTO.Builder> replacedTemplates =
                    generateEntityByTemplateReplaced(template, topology, replacedEntities);
            return Stream.concat(additionTemplates, replacedTemplates);
        });
    }

    /**
     * Generate reservation entities from templates. For reservation entity, it only can add templates
     * , there are no replace templates.
     *
     * @param templateAdditions map key is template id, value is the number of template need to add.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @return set of {@link TopologyEntityDTO} which newly created.
     */
    public Stream<TopologyEntityDTO.Builder> generateReservationEntityFromTemplates(
            @Nonnull final Map<Long, Long> templateAdditions,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final Stream<Template> templates = getTemplatesByIds(templateAdditions.keySet());
        return templates.flatMap(template -> {
            final long additionCount = templateAdditions.getOrDefault(template.getId(), 0L);
            return generateEntityByTemplateAddition(template, topology, additionCount, true);
        });
    }

    /**
     * Get templates from template rpc service by input a set of template ids. If there is any
     * template missing, it will throw {@link TemplatesNotFoundException}.
     *
     * @param templateIds a set of {@link Template} ids.
     * @return stream of templates.
     */
    private Stream<Template> getTemplatesByIds(@Nonnull Set<Long> templateIds) {
        try {
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
            return templates.stream();
        } catch (StatusRuntimeException e) {
            // TODO: (OM-28609) we should have mechanism to notify Plan component that some plan failed
            // at the middle of process (e.g topology processor). In this case, UI or API will not be
            // stuck.
            throw TopologyEditorException.notFoundTemplateException(templateIds);
        }
    }

    private Stream<TopologyEntityDTO.Builder> generateEntityByTemplateAddition(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            final long additionCount,
            final boolean isReservation) {
        return LongStream.range(0L, additionCount)
            .mapToObj(number -> {
                final TopologyEntityDTO.Builder topologyEntityBuilder =
                    TemplatesConverterUtils.generateTopologyEntityBuilder(template);
                topologyEntityBuilder
                    .setOid(identityProvider.generateTopologyId())
                    .setDisplayName(template.getTemplateInfo().getName() + " - Clone #" + number);
                return generateTopologyEntityByType(template, topologyEntityBuilder,
                        topology, null, isReservation);
            });
    }

    private Stream<TopologyEntityDTO.Builder> generateEntityByTemplateReplaced(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Collection<Long> replacedEntities) {
        return replacedEntities.stream()
            .filter(topology::containsKey)
            .map(entityOid -> {
                final TopologyEntity.Builder entityBuilder = topology.get(entityOid);
                // Set shopTogether to true for all consumers of an entity to be replaced.
                // VMs are the only entity types which do shop together. So we filter for VMs here.
                entityBuilder.getConsumers().stream()
                    .filter(c -> c.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    .forEach(consumer -> consumer.getTopologyEntityDtoBuilder()
                        .getAnalysisSettingsBuilder().setShopTogether(true));
                final TopologyEntityDTO.Builder topologyEntityBuilder =
                    TemplatesConverterUtils.generateTopologyEntityBuilder(template);
                topologyEntityBuilder
                    .setOid(identityProvider.generateTopologyId())
                    .setDisplayName(template.getTemplateInfo().getName() + " - Replacing " +
                            entityBuilder.getDisplayName());
                if (entityBuilder.getEntityBuilder().getEditBuilder().hasReplaced()) {
                    entityBuilder.getEntityBuilder().getEditBuilder()
                            .getReplacedBuilder().setReplacementId(topologyEntityBuilder.getOid());

                }
                return generateTopologyEntityByType(template, topologyEntityBuilder, topology,
                    entityOid, false);
            });
    }

    /**
     * Based on different template type, delegate create topology entity logic to different instance.
     *
     * @param template {@link Template} used to create {@link TopologyEntityDTO}.
     * @param topologyEntityBuilder topologyEntity builder contains setting up basic fields.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @param originalTopologyEntityOid the oid of the original topology entity which this template want
     *                  to keep its commodity constrains.
     * @param isReservation if true means generate reservation entity templates, if false means generate normal
     *                      entity from templates. The difference is for reservation entity, it will
     *                      only buy provision commodity.
     * @return The {@link TopologyEntityDTO.Builder} for the newly generated entity.
     */
    private TopologyEntityDTO.Builder generateTopologyEntityByType(
            @Nonnull final Template template,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nullable final Long originalTopologyEntityOid,
            final boolean isReservation) {
        final TopologyEntityDTOOrBuilder originalTopologyEntity = originalTopologyEntityOid == null
                        ? null
                        : topology.get(originalTopologyEntityOid).getEntityBuilder();
        final int templateEntityType = template.getTemplateInfo().getEntityType();
        final Map<Integer, TopologyEntityConstructor> converterMap =
                isReservation ? reservationTemplateConvertMap : templateConverterMap;
        if (!converterMap.containsKey(templateEntityType)) {
            throw new NotImplementedException(templateEntityType + " template is not supported.");
        }
        return converterMap.get(templateEntityType)
            .createTopologyEntityFromTemplate(template, topologyEntityBuilder,
                topology, originalTopologyEntity);
    }
}