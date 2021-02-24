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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.builders.SDKConstants;
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
            throw new TopologyEntityConstructorException(
                    "There are no hosts to replace with the HCI template "
                            + template.getTemplateInfo().getName());
        }

        // Get plan ID from any plan entity
        TopologyEntityDTO.Builder anyPlanEntity = hostsToReplace.iterator().next()
                .getEntityBuilder();
        long planId = anyPlanEntity.getEdit().getReplaced().getPlanId();

        // Create new storage
        TopologyEntityDTO.Builder newStorage = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(template, topology, null,
                        TemplateActionType.REPLACE, identityProvider, null);

        logger.info("Creating HCI storage '{}'", newStorage.getDisplayName());

        // Create Storage Cluster commodity for the new storage
        CommoditySoldDTO.Builder storageClusterComm = TopologyEntityConstructor
                .createAccessCommodity(CommodityDTO.CommodityType.STORAGE_CLUSTER,
                        newStorage.getOid(), null);
        newStorage.addCommoditySoldList(storageClusterComm);
        setStorageClusterCommodities(hostsToReplace, storageClusterComm);

        setResizable(newStorage, CommodityType.STORAGE_AMOUNT);
        setResizable(newStorage, CommodityType.STORAGE_PROVISIONED);
        setStoragePolicy(newStorage);
        result.add(newStorage);

        int reservedHostsCount = (int)getStorageSettingValue(
                EntitySettingSpecs.HciHostCapacityReservation);

        for (int count = 0; count < 1 + reservedHostsCount; count++) {
            String nameSuffix = count == 0 ? null : "(Reserved " + count + ")";

            // Create new host
            TopologyEntityDTO.Builder newHost = new PhysicalMachineEntityConstructor()
                    .createTopologyEntityFromTemplate(template, topology, null,
                            TemplateActionType.REPLACE, identityProvider, nameSuffix);
            result.add(newHost);

            // Process all the hosts
            for (TopologyEntity.Builder hostToReplace : hostsToReplace) {
                logger.info("Replacing host '{}' with '{}'", hostToReplace.getDisplayName(),
                        newHost.getDisplayName());

                TopologyEntityDTO.Builder oldHost = hostToReplace.getEntityBuilder();
                setConstraints(oldHost, newHost);
                TopologyEntityConstructor.addAccess(oldHost, newHost, newStorage);
                setReplacementId(oldHost, newHost.getOid(), planId);

                List<TopologyEntityDTO.Builder> replaceStorages = getProvidingStorages(
                        hostToReplace);
                TopologyEntityDTO.Builder hciStorage = getHCIStorage(hostToReplace);
                replaceStorages.add(hciStorage);
                for (TopologyEntityDTO.Builder storage : replaceStorages) {
                    setReplacementId(storage, newStorage.getOid(), planId);
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
    private List<TopologyEntityDTO.Builder> getProvidingStorages(
            @Nonnull TopologyEntity.Builder host) {
        return host.getProviderIds().stream().map(topology::get)
                .filter(p -> p.getEntityType() == EntityType.STORAGE_VALUE)
                .map(TopologyEntity.Builder::getEntityBuilder).collect(Collectors.toList());
    }

    /**
     * Add Storage Cluster commodities for the VMs and the related non-vSAN
     * storages.
     *
     * @param hostsToReplace hosts to replace
     * @param storageClusterComm Storage Cluster sold commodity
     * @throws TopologyEntityConstructorException error setting the commodities
     */
    private void setStorageClusterCommodities(
            @Nonnull Collection<TopologyEntity.Builder> hostsToReplace,
            @Nonnull CommoditySoldDTO.Builder storageClusterComm)
            throws TopologyEntityConstructorException {
        String key = storageClusterComm.getCommodityType().getKey();

        for (TopologyEntity.Builder host : hostsToReplace) {
            for (TopologyEntity consumer : host.getConsumers()) {
                setStorageClusterCommForVm(consumer.getTopologyEntityDtoBuilder(), key);
            }

            for (Long providerId : host.getProviderIds()) {
                addClusterCommodityToHciStorage(storageClusterComm, providerId);
            }
        }
    }

    private void addClusterCommodityToHciStorage(CommoditySoldDTO.Builder clusterSoldComm,
            Long storageId)
            throws TopologyEntityConstructorException {
        TopologyEntity.Builder storage = topology.get(storageId);

        if (storage == null) {
            throw new TopologyEntityConstructorException("Cannot find  provider " + storageId);
        }

        if (storage.getEntityType() != EntityType.STORAGE_VALUE
                || TopologyEntityConstructor.hasCommodity(storage.getEntityBuilder(),
                        clusterSoldComm.build())) {
            return;
        }

        storage.getEntityBuilder().addCommoditySoldList(clusterSoldComm);
    }

    private void setStorageClusterCommForVm(@Nonnull TopologyEntityDTO.Builder vm,
            @Nonnull String storageClusterKey) {
        if (vm.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return;
        }

        List<CommoditiesBoughtFromProvider.Builder> storageBoughtGroups = vm
                .getCommoditiesBoughtFromProvidersBuilderList().stream()
                .filter(comm -> comm.getProviderEntityType() == EntityType.STORAGE_VALUE)
                .collect(Collectors.toList());

        if (storageBoughtGroups.isEmpty()) {
            logger.warn("VM '{}' does not have any storage providers", vm.getDisplayName());
            return;
        }

        for (CommoditiesBoughtFromProvider.Builder boughtGroup : storageBoughtGroups) {
            CommodityBoughtDTO comm = TopologyEntityConstructor.createCommodityBoughtDTO(
                    CommodityType.STORAGE_CLUSTER_VALUE, storageClusterKey,
                    SDKConstants.ACCESS_COMMODITY_USED);
            boughtGroup.addCommodityBought(comm);
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
