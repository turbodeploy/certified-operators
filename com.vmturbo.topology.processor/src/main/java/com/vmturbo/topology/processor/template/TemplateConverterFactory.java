package com.vmturbo.topology.processor.template;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByIdsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityProvider;

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
     * @param templateIds set of template ids.
     * @return set of {@link TopologyEntityDTO}.
     */
    public Set<TopologyEntityDTO> generateTopologyEntityFromTemplates(@Nonnull final Set<Long> templateIds,
                                                                      @Nonnull final Map<Long, Long> templateAdditions) {
        final Set<TopologyEntityDTO> topologyEntities = new HashSet<>();
        GetTemplatesByIdsRequest getTemplatesRequest = GetTemplatesByIdsRequest.newBuilder()
            .addAllTemplateIds(templateIds)
            .build();
        Iterable<Template> templates = () -> templateService.getTemplatesByIds(getTemplatesRequest);
        StreamSupport.stream(templates.spliterator(), false)
            .forEach(template -> {
                final long additionCount = templateAdditions.getOrDefault(template.getId(), 1L);
                LongStream.range(0L, additionCount)
                    .forEach(number -> {
                        final TopologyEntityDTO.Builder topologyEntityBuilder =
                            TemplatesConverterUtils.generateTopologyEntityBuilder(template);
                        topologyEntityBuilder
                            .setOid(identityProvider.generateTopologyId())
                            .setDisplayName(template.getTemplateInfo().getName() + " - Clone #" + number);
                        generateTopologyEntityByType(template, topologyEntityBuilder, topologyEntities);
                    });
            });
        return topologyEntities;
    }

    /**
     * Based on different template type, delegate create topology entity logic to different instance.
     *
     * @param template {@link Template} used to create {@link TopologyEntityDTO}.
     * @param topologyEntityBuilder builder of {@link TopologyEntityDTO}.
     * @param topologyEntities set of topologyEntities contains all new created topology entities from
     *                         templates.
     */
    private void generateTopologyEntityByType(@Nonnull final Template template,
                                              @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull final Set<TopologyEntityDTO> topologyEntities) {
        final int templateEntityType = template.getTemplateInfo().getEntityType();
        if (!templateConverterMap.containsKey(templateEntityType)) {
            throw new NotImplementedException(templateEntityType + " template is not supported.");
        }
        final TopologyEntityDTO topologyEntityDTO = templateConverterMap.get(templateEntityType)
            .createTopologyEntityFromTemplate(template, topologyEntityBuilder);
        topologyEntities.add(topologyEntityDTO);
    }
}
