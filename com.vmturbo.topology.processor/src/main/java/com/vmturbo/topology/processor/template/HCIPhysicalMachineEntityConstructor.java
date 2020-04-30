package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.jsonwebtoken.lang.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Construct HCI PM and HCI storage out of the HCI template.
 */
public class HCIPhysicalMachineEntityConstructor extends TopologyEntityConstructor {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    private final Template template;
    @Nonnull
    private final Map<Long, TopologyEntity.Builder> topology;
    @Nonnull
    private final Collection<TopologyEntity.Builder> hostsToReplace;
    private final boolean isReplaced;
    @Nonnull
    private final IdentityProvider identityProvider;

    public HCIPhysicalMachineEntityConstructor(@Nonnull Template template,
            @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Collection<TopologyEntity.Builder> hostsToReplace, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        this.template = template;
        this.topology = topology;
        this.hostsToReplace = hostsToReplace;
        this.isReplaced = isReplaced;
        this.identityProvider = identityProvider;

        if (Collections.isEmpty(hostsToReplace)) {
            throw new TopologyEntityConstructorException("HCI template has no hosts to replace");
        }
    }

    /**
     * Create the topology entities from the HCI template.
     *
     * @return created entities
     * @throws TopologyEntityConstructorException error creating entities
     */
    @Nonnull
    public Collection<TopologyEntityDTO.Builder> createTopologyEntitiesFromTemplate()
            throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        // All the hosts should belong to the same vSAN cluster. Get the first
        // one.
        TopologyEntity.Builder originalHost = hostsToReplace.iterator().next();

        TopologyEntityDTO.Builder newHost = new PhysicalMachineEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalHost, isReplaced,
                        identityProvider);
        result.add(newHost);

        logger.info("Replacing HCI host '{}' with '{}'", originalHost.getDisplayName(),
                newHost.getDisplayName());

        long storageOid = getHCIStorageOid(originalHost);
        TopologyEntity.Builder originalStorage = topology.get(storageOid);

        long planId = originalHost.getEntityBuilder().getEdit().getReplaced().getPlanId();
        originalStorage.getEntityBuilder().getEditBuilder().getReplacedBuilder().setPlanId(planId);

        TopologyEntityDTO.Builder newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalStorage, isReplaced,
                        identityProvider);
        result.add(newStorage);

        logger.info("Replacing HCI storage '{}' with '{}'", originalStorage.getDisplayName(),
                newStorage.getDisplayName());

        // Replace oids for the accesses commodities to the new ones
        replaceAccessOid(newStorage, originalHost.getEntityBuilder(), newHost);
        replaceAccessOid(newHost, originalStorage.getEntityBuilder(), newStorage);

        // Create commodities from the template
        Map<String, String> templateMap = createFieldNameValueMap(
                getTemplateResources(template, ResourcesCategoryName.Storage));
        addStorageCommoditiesSold(newHost, templateMap);
        addStorageCommoditiesBought(newStorage, newHost.getOid(), templateMap);

        // Set Replace for the PM related storages
        for (Long providerOid : originalHost.getProviderIds()) {
            TopologyEntity.Builder provider = topology.get(providerOid);

            if (provider.getEntityType() != EntityType.STORAGE_VALUE) {
                continue;
            }

            provider.getEntityBuilder().getEditBuilder().getReplacedBuilder()
                    .setReplacementId(newStorage.getOid()).setPlanId(planId);
            replaceAccessOid(provider.getEntityBuilder(), originalHost.getEntityBuilder(), newHost);

            logger.trace("Marked Storage '{}' for replacement with '{}'", provider.getDisplayName(),
                    newStorage.getDisplayName());
        }

        setClusterCommodities(newStorage);

        return result;
    }

    /**
     * Set Cluster commodity key for the VMs and the related non-vSAN storages
     * to the vSAN cluster key.
     *
     * @param newStorage new vSAN storage
     * @throws TopologyEntityConstructorException error setting the commodities
     */
    private void setClusterCommodities(@Nonnull TopologyEntityDTO.Builder newStorage)
            throws TopologyEntityConstructorException {
        String clusterKey = getStorageClusterKey(newStorage);
        TopologyDTO.CommodityType clusterTypeWithKey = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.STORAGE_CLUSTER_VALUE).setKey(clusterKey).build();

        for (TopologyEntity.Builder host : hostsToReplace) {
            for (TopologyEntity consumer : host.getConsumers()) {
                if (consumer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    setClusterKeyForVm(consumer.getTopologyEntityDtoBuilder(), clusterTypeWithKey);
                }
            }

            for (Long providerId : host.getProviderIds()) {
                TopologyEntity.Builder provider = topology.get(providerId);
                if (provider.getEntityType() == EntityType.STORAGE_VALUE) {
                    setClusterKeyForStorage(provider.build(), clusterTypeWithKey);
                }
            }
        }
    }

    private void setClusterKeyForVm(@Nonnull TopologyEntityDTO.Builder vm,
            @Nonnull TopologyDTO.CommodityType clusterTypeWithKey)
            throws TopologyEntityConstructorException {
        List<CommoditiesBoughtFromProvider.Builder> boughtComms = vm
                .getCommoditiesBoughtFromProvidersBuilderList().stream()
                .filter(comm -> comm.getProviderEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());

        if (boughtComms.isEmpty()) {
            throw new TopologyEntityConstructorException(
                    "The VM '" + vm.getDisplayName() + "' does not buy from any storages");
        }

        for (CommoditiesBoughtFromProvider.Builder boughtComm : boughtComms) {
            for (CommodityBoughtDTO.Builder comm : boughtComm.getCommodityBoughtBuilderList()) {
                if (comm.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE) {
                    comm.setCommodityType(clusterTypeWithKey);
                }
            }
        }
    }

    private void setClusterKeyForStorage(@Nonnull TopologyEntity storage,
            @Nonnull TopologyDTO.CommodityType clusterTypeWithKey)
            throws TopologyEntityConstructorException {
        CommoditySoldDTO.Builder clusterComm = getSoldCommodity(
                storage.getTopologyEntityDtoBuilder(), CommodityType.STORAGE_CLUSTER);

        clusterComm.setCommodityType(clusterTypeWithKey);
    }

    @Nonnull
    private String getStorageClusterKey(@Nonnull TopologyEntityDTO.Builder newStorage)
            throws TopologyEntityConstructorException {
        CommoditySoldDTO.Builder clusterComm = getSoldCommodity(newStorage,
                CommodityType.STORAGE_CLUSTER);

        return clusterComm.getCommodityType().getKey();
    }

    private CommoditySoldDTO.Builder getSoldCommodity(TopologyEntityDTO.Builder entity,
            CommodityDTO.CommodityType commType) throws TopologyEntityConstructorException {
        List<CommoditySoldDTO.Builder> clusterComms = entity.getCommoditySoldListBuilderList()
                .stream().filter(comm -> comm.getCommodityType().getType() == commType.getNumber())
                .collect(Collectors.toList());

        if (clusterComms.size() != 1) {
            throw new TopologyEntityConstructorException(
                    "The entity '" + entity.getDisplayName() + "' should have one " + commType
                            + " commodity, but has " + clusterComms.size());
        }

        return clusterComms.get(0);
    }

    /**
     * Replace oids for the 'assesses' fields to the new ones.
     *
     * @param entity entity to modify
     * @param oldAccessEntity old oid
     * @param newAccessEntity new oid
     */
    private static void replaceAccessOid(@Nonnull TopologyEntityDTO.Builder entity,
            TopologyEntityDTO.Builder oldAccessEntity, TopologyEntityDTO.Builder newAccessEntity) {
        for (CommoditySoldDTO.Builder comm : entity.getCommoditySoldListBuilderList()) {
            if (comm.getAccesses() == oldAccessEntity.getOid()) {
                comm.setAccesses(newAccessEntity.getOid());
                logger.info("Entity: '{}'. Change {} accesses from '{}' to '{}'",
                        entity.getDisplayName(), commodityToString(comm),
                        oldAccessEntity.getDisplayName(), newAccessEntity.getDisplayName());
            }
        }
    }

    @Nonnull
    private static String commodityToString(@Nonnull CommoditySoldDTO.Builder comm) {
        return "[" + comm.getCommodityType().getType() + "-"
                + convertCommodityType(comm.getCommodityType()) + ", key: '"
                + comm.getCommodityType().getKey() + "']";
    }

    @Nonnull
    private static CommodityDTO.CommodityType convertCommodityType(
            @Nonnull TopologyDTO.CommodityType commodityType) {
        return CommodityType.internalGetValueMap().findValueByNumber(commodityType.getType());
    }

    private static long getHCIStorageOid(@Nonnull TopologyEntity.Builder hciHost)
            throws TopologyEntityConstructorException {
        List<TopologyEntity> storages = hciHost.getConsumers().stream()
                .filter(consumer -> consumer.getEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());

        if (storages.size() != 1) {
            throw new TopologyEntityConstructorException("The HCI host '" + hciHost.getDisplayName()
                    + "' should have one Storage consumer, but has " + storages.size());
        }

        return storages.get(0).getOid();
    }
}
