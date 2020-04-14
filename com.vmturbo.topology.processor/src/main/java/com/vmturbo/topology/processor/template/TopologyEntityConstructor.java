package com.vmturbo.topology.processor.template;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Responsible for creating a TopologyEntityDTO from TemplateDTO. And those
 * entities should be unplaced. And also it will try to keep all commodity
 * constrains from the original topology entity.
 */
public class TopologyEntityConstructor {


    /**
     * Create topology entities from a template. It modifies the original entity
     * with the reference to the new one.
     *
     * @param template template
     * @param topology topology
     * @param originalTopologyEntity original TopologyEntity
     * @param isReplaced is replaced
     * @param identityProvider identity provider
     * @return topology entities
     * @throws TopologyEntityConstructorException error creating topology
     *             entities
     */
    public Collection<TopologyEntityDTO.Builder> createTopologyEntityFromTemplate(
            @Nonnull Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Optional<TopologyEntity.Builder> originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder result = generateTopologyEntityBuilder(template);

        long oid = identityProvider.generateTopologyId();
        result.setOid(oid);

        String actionName = isReplaced ? " - Replacing " : " - Cloning ";
        String displayName = template.getTemplateInfo().getName() + actionName;

        if (originalTopologyEntity.isPresent()) {
            // Modify original topology entity.
            if (isReplaced) {
                originalTopologyEntity.get().getEntityBuilder().getEditBuilder()
                        .getReplacedBuilder().setReplacementId(oid);
            }

            displayName += originalTopologyEntity.get().getDisplayName();
        }

        result.setDisplayName(displayName);

        return Collections.singletonList(result);
    }

    /**
     * Generate a {@link TopologyEntityDTO} builder contains common fields
     * between different templates.
     *
     * @param template {@link Template}
     * @return {@link TopologyEntityDTO} builder
     */
    private static TopologyEntityDTO.Builder generateTopologyEntityBuilder(
            @Nonnull final Template template) {
        TemplateInfo templateInfo = template.getTemplateInfo();
        return TopologyEntityDTO.newBuilder().setEntityType(templateInfo.getEntityType())
                .setEntityState(EntityState.POWERED_ON).setAnalysisSettings(AnalysisSettings
                        .newBuilder().setIsAvailableAsProvider(true).setShopTogether(true));
    }
}
