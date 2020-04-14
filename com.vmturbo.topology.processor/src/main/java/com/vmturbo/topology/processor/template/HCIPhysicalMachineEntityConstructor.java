package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Construct HCI PM and HCI storage out of the HCI template.
 */
public class HCIPhysicalMachineEntityConstructor extends TopologyEntityConstructor {

    @Override
    public Collection<TopologyEntityDTO.Builder> createTopologyEntityFromTemplate(
            @Nonnull Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Optional<TopologyEntity.Builder> originalPM, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        if (!originalPM.isPresent()) {
            throw new TopologyEntityConstructorException(
                    "Original PM is missing. Template: " + template.getTemplateInfo().getName());
        }

        Template.Builder pmTemplate = template.toBuilder();
        pmTemplate.getTemplateInfoBuilder().setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);

        Collection<TopologyEntityDTO.Builder> newPM = new PhysicalMachineEntityConstructor()
                .createTopologyEntityFromTemplate(pmTemplate.build(), topology, originalPM,
                        isReplaced, identityProvider);
        result.addAll(newPM);

        Template.Builder storageTemplate = template.toBuilder();
        storageTemplate.getTemplateInfoBuilder().setEntityType(EntityType.STORAGE_VALUE);

        long storageOid = getHCIStorageOid(originalPM.get());
        TopologyEntity.Builder originalStorage = topology.get(storageOid);

        long planId = originalPM.get().getEntityBuilder().getEdit().getReplaced().getPlanId();
        originalStorage.getEntityBuilder().getEditBuilder().getReplacedBuilder().setPlanId(planId);

        Collection<TopologyEntityDTO.Builder> newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(storageTemplate.build(), topology,
                        Optional.of(originalStorage), isReplaced, identityProvider);
        result.addAll(newStorage);

        return result;
    }

    private static long getHCIStorageOid(@Nonnull TopologyEntity.Builder originalPM)
            throws TopologyEntityConstructorException {
        for (TopologyEntity consumer : originalPM.getConsumers()) {
            if (consumer.getEntityType() == EntityType.STORAGE_VALUE) {
                return consumer.getOid();
            }
        }

        throw new TopologyEntityConstructorException(
                "PM '" + originalPM.getDisplayName() + "' does not have a Storage consumer");
    }
}
