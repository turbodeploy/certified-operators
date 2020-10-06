package com.vmturbo.topology.processor.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StoragePolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageRedundancyMethod;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor.TemplateActionType;

/**
 * Construct HCI host and HCI storage out of the HCI template.
 */
public class HCIPhysicalMachineEntityConstructor {

    private static final Logger logger = LogManager.getLogger();

    private final Template template;
    private final Map<Long, TopologyEntity.Builder> topology;
    private final IdentityProvider identityProvider;
    private final Collection<Setting> storageSettings;
    private final Map<String, String> templateValues;

    public HCIPhysicalMachineEntityConstructor(@Nonnull Template template,
            @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nonnull IdentityProvider identityProvider,
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService)
            throws TopologyEntityConstructorException {
        this.template = template;
        this.topology = topology;
        this.identityProvider = identityProvider;
        this.templateValues = TopologyEntityConstructor.createFieldNameValueMap(template,
                ResourcesCategoryName.Storage);
        this.storageSettings = getStorageSettings(settingPolicyService);
    }

    /**
     * Create the topology entities from the HCI template.
     *
     * @param hostsToReplace hosts to replace
     * @return created entities
     * @throws TopologyEntityConstructorException error creating entities
     */
    @Nonnull
    public Collection<TopologyEntityDTO.Builder> replaceEntitiesFromTemplate(
            @Nonnull Collection<TopologyEntity.Builder> hostsToReplace)
            throws TopologyEntityConstructorException {
        List<TopologyEntityDTO.Builder> result = new ArrayList<>();

        if (hostsToReplace.isEmpty()) {
            throw new TopologyEntityConstructorException("HCI template has no hosts to replace");
        }

        // All the hosts should belong to the same vSAN cluster. Get the first
        // one.
        TopologyEntity.Builder anyHost = hostsToReplace.iterator().next();
        TopologyEntityDTO.Builder oldStorage = getHCIStorage(anyHost);

        // Create new storage
        TopologyEntityDTO.Builder newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(template, topology, oldStorage,
                        TemplateActionType.REPLACE, identityProvider, null);

        if (oldStorage == null) {
            logger.info("Creating HCI storage '{}'", newStorage.getDisplayName());

            // Create fake Storage cluster commodity
            CommoditySoldDTO.Builder comm = TopologyEntityConstructor.createAccessCommodity(
                    CommodityDTO.CommodityType.STORAGE_CLUSTER, newStorage.getOid(), null);
            newStorage.addCommoditySoldList(comm);
        } else {
            logger.info("Replacing HCI storage '{}' with '{}'", oldStorage.getDisplayName(),
                    newStorage.getDisplayName());
        }

        setClusterCommodities(hostsToReplace, newStorage);
        setResizable(newStorage, CommodityType.STORAGE_AMOUNT);
        setResizable(newStorage, CommodityType.STORAGE_PROVISIONED);
        long planId = anyHost.getEntityBuilder().getEdit().getReplaced().getPlanId();
        setReplacementId(oldStorage, newStorage.getOid(), planId);
        setStoragePolicy(newStorage);
        result.add(newStorage);

        int reservedHostsCount = (int)getStorageSettingValue(
                EntitySettingSpecs.HciHostCapacityReservation);

        for (int count = 0; count < 1 + reservedHostsCount; count++) {
            String nameSuffix = count == 0 ? null : "(Reserved " + count + ")";

            // Create new host
            TopologyEntityDTO.Builder newHost = new PhysicalMachineEntityConstructor()
                    .createTopologyEntityFromTemplate(template, topology,
                            anyHost.getEntityBuilder(), TemplateActionType.REPLACE,
                            identityProvider, nameSuffix);
            result.add(newHost);
            logger.info("Replacing HCI host '{}' with '{}'", anyHost.getDisplayName(),
                    newHost.getDisplayName());

            // Process all the hosts
            for (TopologyEntity.Builder hostToReplace : hostsToReplace) {
                TopologyEntityDTO.Builder oldHost = hostToReplace.getEntityBuilder();
                setConstraints(oldHost, newHost);
                TopologyEntityConstructor.addAccess(oldHost, newHost,
                        newStorage);
                setReplacementId(oldHost, newHost.getOid(), planId);

                for (TopologyEntity.Builder provider : getProvidingStorages(hostToReplace)) {
                    setReplacementId(provider.getEntityBuilder(), newStorage.getOid(), planId);
                }
            }

            // Create commodities from the template
            // HCI Hosts sell storage commodities. They are providing storage to
            // the vSAN datastore
            TopologyEntityConstructor.addStorageCommoditiesSold(newHost, templateValues, true);
            TopologyEntityConstructor.addStorageCommoditiesBought(newStorage, newHost.getOid(),
                    templateValues);
        }

        return result;
    }

    private static void setReplacementId(@Nullable TopologyEntityDTO.Builder entity, long id,
            long planId) {
        if (entity == null) {
            return;
        }

        entity.getEditBuilder().getReplacedBuilder().setReplacementId(id).setPlanId(planId);
    }

    private static void setResizable(@Nonnull TopologyEntityDTO.Builder newStorage,
            @Nonnull CommodityDTO.CommodityType commType)
            throws TopologyEntityConstructorException {
        CommoditySoldDTO.Builder comm = getSoldCommodityUnique(newStorage, commType);
        comm.setIsResizeable(true);
    }

    /**
     * Copy sold access commodities from the original host to the new one.
     * Update access relationships.
     *
     * @param newHost new host
     * @param oldHost original host
     * @throws TopologyEntityConstructorException error setting access
     *             commodities
     */
    private void setConstraints(@Nonnull TopologyEntityDTO.Builder oldHost,
            @Nonnull TopologyEntityDTO.Builder newHost) throws TopologyEntityConstructorException {
        Set<CommoditySoldDTO> oldHostConstraints = TopologyEntityConstructor
                .getCommoditySoldConstraint(oldHost);

        for (CommoditySoldDTO oldHostComm : oldHostConstraints) {
            // Do not add the same commodity more then once
            if (TopologyEntityConstructor.hasCommodity(newHost, oldHostComm)) {
                continue;
            }

            newHost.addCommoditySoldList(oldHostComm);
        }

        TopologyEntityConstructor.updateRelatedEntityAccesses(oldHost, newHost, oldHostConstraints,
                topology);
    }

    /**
     * Set the new vSAN storage policy by the HCI settings.
     *
     * @param newStorage vSAN storage
     * @throws TopologyEntityConstructorException error in HCI processing
     */
    private void setStoragePolicy(@Nonnull TopologyEntityDTO.Builder newStorage)
            throws TopologyEntityConstructorException {
        StorageInfo.Builder storage = newStorage.getTypeSpecificInfoBuilder().getStorageBuilder();
        storage.setStorageType(StorageType.VSAN);
        storage.setIsLocal(false);
        StoragePolicy.Builder policy = storage.getPolicyBuilder();

        int failuresToTolerate = TopologyEntityConstructor
                .getTemplateValue(templateValues, "failuresToTolerate").intValue();
        policy.setFailuresToTolerate(failuresToTolerate);
        policy.setRedundancy(getRaidLevel());
    }

    private StorageRedundancyMethod getRaidLevel() throws TopologyEntityConstructorException {
        int redundancyMethod = TopologyEntityConstructor
                .getTemplateValue(templateValues, "redundancyMethod").intValue();
        StorageRedundancyMethod result = StorageRedundancyMethod.forNumber(redundancyMethod);

        if (result == null) {
            throw new TopologyEntityConstructorException(
                    "Invalid redundancy method value: " + redundancyMethod);
        }

        return result;
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
     * Add Cluster commodities for the VMs and the related non-vSAN storages.
     *
     * @param hostsToReplace hosts to replace
     * @param newStorage new vSAN storage
     * @throws TopologyEntityConstructorException error setting the commodities
     */
    private void setClusterCommodities(@Nonnull Collection<TopologyEntity.Builder> hostsToReplace,
            @Nonnull TopologyEntityDTO.Builder newStorage)
            throws TopologyEntityConstructorException {
        List<CommoditySoldDTO.Builder> clusterSoldComms = getSoldCommodities(newStorage,
                CommodityType.STORAGE_CLUSTER);

        for (CommoditySoldDTO.Builder clusterSoldComm : clusterSoldComms) {
            String clusterKey = clusterSoldComm.getCommodityType().getKey();
            TopologyDTO.CommodityType clusterTypeWithKey = TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.STORAGE_CLUSTER_VALUE).setKey(clusterKey).build();

            for (TopologyEntity.Builder host : hostsToReplace) {
                for (TopologyEntity consumer : host.getConsumers()) {
                    setClusterKeyForVm(consumer.getTopologyEntityDtoBuilder(), clusterTypeWithKey);
                }

                for (Long providerId : host.getProviderIds()) {
                    setClusterKeyForProvider(clusterSoldComm, host, providerId);
                }
            }
        }
    }

    private void setClusterKeyForProvider(CommoditySoldDTO.Builder clusterSoldComm,
            TopologyEntity.Builder host, Long providerId)
            throws TopologyEntityConstructorException {
        TopologyEntity.Builder provider = topology.get(providerId);

        if (provider == null) {
            throw new TopologyEntityConstructorException("Cannot find  provider "
                    + providerId + " for host " + host.getDisplayName());
        }

        if (provider.getEntityType() != EntityType.STORAGE_VALUE
                || TopologyEntityConstructor.hasCommodity(provider.getEntityBuilder(),
                        clusterSoldComm.build())) {
            return;
        }

        provider.getEntityBuilder().addCommoditySoldList(clusterSoldComm);
    }

    private void setClusterKeyForVm(@Nonnull TopologyEntityDTO.Builder vm,
            @Nonnull TopologyDTO.CommodityType clusterTypeWithKey)
            throws TopologyEntityConstructorException {
        if (vm.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return;
        }

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

    @Nonnull
    private static List<CommoditySoldDTO.Builder> getSoldCommodities(
            @Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull CommodityDTO.CommodityType commType) {
        return entity.getCommoditySoldListBuilderList().stream()
                .filter(comm -> comm.getCommodityType().getType() == commType.getNumber())
                .collect(Collectors.toList());
    }

    @Nonnull
    private static CommoditySoldDTO.Builder getSoldCommodityUnique(
            @Nonnull TopologyEntityDTO.Builder entity, @Nonnull CommodityDTO.CommodityType commType)
            throws TopologyEntityConstructorException {
        List<CommoditySoldDTO.Builder> clusterComms = getSoldCommodities(entity, commType);

        if (clusterComms.size() != 1) {
            throw new TopologyEntityConstructorException(
                    "The entity '" + entity.getDisplayName() + "' should have one " + commType
                            + " commodity, but has " + clusterComms.size());
        }

        return clusterComms.get(0);
    }

    @Nonnull
    private static String commodityToString(@Nonnull CommoditySoldDTO.Builder comm) {
        return "[" + comm.getCommodityType().getType() + "-"
                + TopologyEntityConstructor.convertCommodityType(comm.getCommodityType())
                + ", key: '" + comm.getCommodityType().getKey() + "']";
    }

    @Nullable
    private TopologyEntityDTO.Builder getHCIStorage(@Nonnull TopologyEntity.Builder hciHost)
            throws TopologyEntityConstructorException {
        List<TopologyEntity> storages = hciHost.getConsumers().stream()
                .filter(consumer -> consumer.getEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());

        if (storages.isEmpty()) {
            return null;
        }

        if (storages.size() != 1) {
            throw new TopologyEntityConstructorException("The HCI host '" + hciHost.getDisplayName()
                    + "' should have one Storage consumer, but has " + storages.size());
        }

        long storageOid = storages.get(0).getOid();
        return topology.get(storageOid).getEntityBuilder();
    }

    private float getStorageSettingValue(@Nonnull EntitySettingSpecs settingSpec)
            throws TopologyEntityConstructorException {
        for (Setting setting : storageSettings) {
            if (setting.getSettingSpecName().equals(settingSpec.getSettingName())) {
                return setting.getNumericSettingValue().getValue();
            }
        }

        throw new TopologyEntityConstructorException("Setting is not found: " + settingSpec);
    }

    @Nonnull
    private static List<Setting> getStorageSettings(
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService)
            throws TopologyEntityConstructorException {
        ListSettingPoliciesRequest.Builder reqBuilder = ListSettingPoliciesRequest.newBuilder()
                .setTypeFilter(Type.DEFAULT);

        List<SettingPolicy> settingPolicies = new ArrayList<>();
        settingPolicyService.listSettingPolicies(reqBuilder.build())
                .forEachRemaining(settingPolicies::add);

        Optional<SettingPolicy> storageSettingPolicy = settingPolicies.stream()
                .filter(p -> p.getInfo().getEntityType() == EntityType.STORAGE_VALUE).findFirst();

        if (!storageSettingPolicy.isPresent()) {
            throw new TopologyEntityConstructorException(
                    "Error retrieving Storage setting policies. Setting policies size: "
                            + settingPolicies.size());
        }

        return storageSettingPolicy.get().getInfo().getSettingsList();
    }
}
