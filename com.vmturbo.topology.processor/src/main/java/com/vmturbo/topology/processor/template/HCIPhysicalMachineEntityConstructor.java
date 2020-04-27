package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Construct HCI PM and HCI storage out of the HCI template.
 */
public class HCIPhysicalMachineEntityConstructor extends TopologyEntityConstructor {

    private static final Logger logger = LogManager.getLogger();

    @Override
    @Nonnull
    public Collection<TopologyEntityDTO.Builder> createTopologyEntityFromTemplate(
            @Nonnull Template template, @Nullable Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntity.Builder originalHost, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        if (originalHost == null) {
            throw new TopologyEntityConstructorException(
                    "Original PM is missing. Template: " + template.getTemplateInfo().getName());
        }

        if (topology == null) {
            throw new TopologyEntityConstructorException(
                    "Topology is missing. Template: " + template.getTemplateInfo().getName());
        }

        TopologyEntityDTO.Builder newHost = new PhysicalMachineEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalHost, isReplaced,
                        identityProvider)
                .iterator().next();
        result.add(newHost);

        long storageOid = getHCIStorageOid(originalHost);
        TopologyEntity.Builder originalStorage = topology.get(storageOid);

        long planId = originalHost.getEntityBuilder().getEdit().getReplaced().getPlanId();
        originalStorage.getEntityBuilder().getEditBuilder().getReplacedBuilder().setPlanId(planId);

        TopologyEntityDTO.Builder newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalStorage, isReplaced,
                        identityProvider)
                .iterator().next();
        result.add(newStorage);

        // Replace oids for the accesses commodities to the new ones
        replaceAccessOid(newStorage, originalHost.getOid(), newHost.getOid());
        replaceAccessOid(newHost, originalStorage.getOid(), newStorage.getOid());

        // Create commodities from the template
        Map<String, String> templateMap = createFieldNameValueMap(
                getTemplateResources(template, ResourcesCategoryName.Storage));
        addStorageCommoditiesSold(newHost, templateMap);
        addStorageCommoditiesBought(newStorage, newHost.getOid(), templateMap);

        // Set Replace for the PM related storages
        for (Long providerOid : originalHost.getProviderIds()) {
            Builder provider = topology.get(providerOid);

            if (provider.getEntityType() != EntityType.STORAGE_VALUE) {
                continue;
            }

            provider.getEntityBuilder().getEditBuilder().getReplacedBuilder()
                    .setReplacementId(newStorage.getOid()).setPlanId(planId);

            logger.trace("Marked Storage '{}' for replacement with '{}'", provider.getDisplayName(),
                    newStorage.getDisplayName());
        }

        return result;
    }

    private static void replaceAccessOid(@Nonnull TopologyEntityDTO.Builder entity, long oldOid,
            long newOid) {
        for (CommoditySoldDTO.Builder comm : entity.getCommoditySoldListBuilderList()) {
            if (comm.getAccesses() == oldOid) {
                comm.setAccesses(newOid);
            }
        }
    }

    private static long getHCIStorageOid(@Nonnull TopologyEntity.Builder originalHost)
            throws TopologyEntityConstructorException {
        List<TopologyEntity> storages = originalHost.getConsumers().stream()
                .filter(consumer -> consumer.getEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());

        if (storages.size() != 1) {
            throw new TopologyEntityConstructorException("PM '" + originalHost.getDisplayName()
                    + "' should have one Storage consumer, but has " + storages.size());
        }

        return storages.get(0).getOid();
    }
}
