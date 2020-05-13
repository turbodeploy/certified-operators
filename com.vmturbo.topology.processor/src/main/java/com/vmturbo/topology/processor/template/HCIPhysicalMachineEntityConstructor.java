package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StoragePolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageRedundancyMethod;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Construct HCI host and HCI storage out of the HCI template.
 */
public class HCIPhysicalMachineEntityConstructor {

    private static final Logger logger = LogManager.getLogger();

    private final Template template;
    private final Map<Long, TopologyEntity.Builder> topology;
    private final Collection<TopologyEntity.Builder> hostsToReplace;
    private final boolean isReplaced;
    private final IdentityProvider identityProvider;

    private final TopologyEntity.Builder originalHost;
    private final TopologyEntity.Builder originalStorage;
    private final Map<String, String> templateValues;

    public HCIPhysicalMachineEntityConstructor(@Nonnull Template template,
            @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Collection<TopologyEntity.Builder> hostsToReplace, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        this.template = template;
        this.topology = topology;
        this.hostsToReplace = hostsToReplace;
        this.isReplaced = isReplaced;
        this.identityProvider = identityProvider;

        if (hostsToReplace.isEmpty()) {
            throw new TopologyEntityConstructorException("HCI template has no hosts to replace");
        }

        // All the hosts should belong to the same vSAN cluster. Get the first
        // one.
        this.originalHost = hostsToReplace.iterator().next();
        this.originalStorage = topology.get(getHCIStorageOid(originalHost));
        this.templateValues = TopologyEntityConstructor.createFieldNameValueMap(template,
                ResourcesCategoryName.Storage);
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

        // Create new storage
        TopologyEntityDTO.Builder newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalStorage, isReplaced,
                        identityProvider);
        setClusterCommodities(newStorage);
        setReplaced(newStorage);
        setStoragePolicy(newStorage);
        result.add(newStorage);
        logger.info("Replacing HCI storage '{}' with '{}'", originalStorage.getDisplayName(),
                newStorage.getDisplayName());

        // Create new host
        TopologyEntityDTO.Builder newHost = new PhysicalMachineEntityConstructor()
                .createTopologyEntityFromTemplate(template, null, originalHost, isReplaced,
                        identityProvider);
        result.add(newHost);
        logger.info("Replacing HCI host '{}' with '{}'", originalHost.getDisplayName(),
                newHost.getDisplayName());

        // Replace oids for the accesses commodities to the new ones
        replaceAccessOid(newStorage, originalHost.getOid(), newHost.getOid());
        replaceAccessOid(newHost, originalStorage.getOid(), newStorage.getOid());

        for (TopologyEntity.Builder provider : getProvidingStorages(originalHost)) {
            replaceAccessOid(provider.getEntityBuilder(), originalHost.getOid(), newHost.getOid());
        }

        // Create commodities from the template
        TopologyEntityConstructor.addStorageCommoditiesSold(newHost, templateValues, true);
        TopologyEntityConstructor.addStorageCommoditiesBought(newStorage, newHost.getOid(),
                templateValues);

        return result;
    }

    /**
     * Set the new vSAN storage commodities by the HCI settings.
     *
     * @param newStorage vSAN storage
     * @throws TopologyEntityConstructorException error in HCI processing
     */
    private void setStoragePolicy(@Nonnull TopologyEntityDTO.Builder newStorage)
            throws TopologyEntityConstructorException {
        StoragePolicy.Builder policy = newStorage.getTypeSpecificInfoBuilder().getStorageBuilder()
                .getPolicyBuilder();

        int failuresToTolerate = TopologyEntityConstructor
                .getTemplateValue(templateValues, "failuresToTolerate").intValue();
        policy.setFailuresToTolerate(failuresToTolerate);
        policy.setRedundancy(getRaidLevel());
    }

    // TODO Re-implement the method when OM-58198 is fixed
    private StorageRedundancyMethod getRaidLevel() throws TopologyEntityConstructorException {
        int failuresToTolerate = TopologyEntityConstructor
                .getTemplateValue(templateValues, "failuresToTolerate").intValue();
        int redundancyMethod = TopologyEntityConstructor
                .getTemplateValue(templateValues, "redundancyMethod").intValue();

        // RAID0
        if (failuresToTolerate == 0) {
            return StorageRedundancyMethod.RAID0;
        }

        // RAID1
        if (redundancyMethod == 0) {
            return StorageRedundancyMethod.RAID1;
        }

        // RAID5
        if (redundancyMethod == 1) {
            return StorageRedundancyMethod.RAID5;
        }

        // RAID6
        if (redundancyMethod == 2) {
            return StorageRedundancyMethod.RAID6;
        }

        throw new TopologyEntityConstructorException("Invalid combination: Failures to tolerate: "
                + failuresToTolerate + ", redundancy method: " + redundancyMethod);
    }

    /**
     * Set the 'replaced' fields.
     *
     * @param newStorage new vSAN storage
     */
    private void setReplaced(@Nonnull TopologyEntityDTO.Builder newStorage) {
        long planId = originalHost.getEntityBuilder().getEdit().getReplaced().getPlanId();
        originalStorage.getEntityBuilder().getEditBuilder().getReplacedBuilder().setPlanId(planId);

        for (TopologyEntity.Builder provider : getProvidingStorages(originalHost)) {
            provider.getEntityBuilder().getEditBuilder().getReplacedBuilder()
                    .setReplacementId(newStorage.getOid()).setPlanId(planId);
        }
    }

    /**
     * Get the storages that are the providers for the host.
     *
     * @param host host
     * @return storages providers for the host
     */
    @Nonnull
    private List<TopologyEntity.Builder> getProvidingStorages(
            @Nonnull TopologyEntity.Builder host) {
        return host.getProviderIds().stream().map(topology::get)
                .filter(p -> p.getEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());
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
     * @param oldOid old oid
     * @param newOid new oid
     */
    private static void replaceAccessOid(@Nonnull TopologyEntityDTO.Builder entity, long oldOid,
            long newOid) {
        for (CommoditySoldDTO.Builder comm : entity.getCommoditySoldListBuilderList()) {
            if (comm.getAccesses() == oldOid) {
                comm.setAccesses(newOid);
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
