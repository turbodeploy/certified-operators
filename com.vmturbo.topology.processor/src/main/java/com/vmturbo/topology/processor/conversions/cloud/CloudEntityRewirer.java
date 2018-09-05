package com.vmturbo.topology.processor.conversions.cloud;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.conversions.Converter;
import com.vmturbo.topology.processor.stitching.CommoditySoldMerger;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;

/**
 * Handles re-wiring of cloud entity objects, as part of the conversion from classic "fake" cloud
 * entities model to the new cloud model in XL. "Rewire" means changing the entities coming from
 * the cloud probe and the relationship between them, it includes: change provider to a different
 * one, add or remove new commodities, add new relationship like "connectedTo", "owns", remove
 * entity from topology, etc.
 */
public class CloudEntityRewirer {

    private static final Logger logger = LogManager.getLogger();

    private static final Map<EntityType, EntityRewirer> CLOUD_REWIRERS;

    static {
        // call in the rewiring squad.
        final Map<EntityType, EntityRewirer> rewirers = new EnumMap<>(EntityType.class);
        rewirers.put(EntityType.VIRTUAL_MACHINE, new VirtualMachineRewirer());
        rewirers.put(EntityType.AVAILABILITY_ZONE, new AvailabilityZoneRewirer());
        rewirers.put(EntityType.REGION, new RegionRewirer());
        rewirers.put(EntityType.STORAGE, new StorageRewirer());
        rewirers.put(EntityType.COMPUTE_TIER, new ComputeTierRewirer());
        rewirers.put(EntityType.DATABASE_TIER, new DatabaseTierRewirer());
        rewirers.put(EntityType.DATABASE, new DatabaseRewirer());
        rewirers.put(EntityType.DATABASE_SERVER, new DatabaseServerRewirer());
        rewirers.put(EntityType.BUSINESS_ACCOUNT, new BusinessAccountRewirer());
        rewirers.put(EntityType.LOAD_BALANCER, new LoadBalancerRewirer());
        rewirers.put(EntityType.APPLICATION, new ApplicationRewirer());
        rewirers.put(EntityType.VIRTUAL_APPLICATION, new VirtualApplicationRewirer());
        rewirers.put(EntityType.DISK_ARRAY, new DiskArrayRewirer());
        CLOUD_REWIRERS = Collections.unmodifiableMap(rewirers);
    }

    private static final EntityRewirer defaultRewirer = new DefaultRewirer();

    /**
     * Starting point in this class to rewire a given entity.
     *
     * @param entity the {@link TopologyStitchingEntity} to rewire
     * @param entitiesByLocalId all entities discovered by the this entity's target, mapped by local id
     * @param stitchingGraph the stitching graph that is under construction
     * @param businessAccountLocalIdsByTargetId business accounts local ids for different target
     * @param sharedEntities the shared entities used by different cloud targets
     * @return true, if the entity should be removed from the topology.
     */
    public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
            @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
            @Nonnull final TopologyStitchingGraph stitchingGraph,
            @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
            @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
        return CLOUD_REWIRERS.getOrDefault(entity.getEntityType(), defaultRewirer).rewire(entity,
                entitiesByLocalId, stitchingGraph, businessAccountLocalIdsByTargetId, sharedEntities);
    }

    /**
     * Utility method for adding the given entity to be owned by BusinessAccount. Currently only
     * following entities are owned by BA:
     *     VM, DB, DBS, App, vApp, LoadBalancer
     * Todo: We expect one entity to be owned by only one BusinessAccount. There are shared
     * entities between accounts, but we don't add them to be owned by BA. If they can be
     * discovered by two BAs, then we need to change this logic so one entity is not owned by
     * two BAs.
     *
     * @param entity the {@link TopologyStitchingEntity} to be owned by BusinessAccount
     * @param stitchingGraph the stitching graph that is under construction
     * @param businessAccountLocalIdsByTargetId business accounts local ids for different target
     * @param entitiesByLocalId all entities discovered by the this entity's target, mapped by local id
     */
    private static void ownedByBusinessAccount(@Nonnull final TopologyStitchingEntity entity,
            @Nonnull final TopologyStitchingGraph stitchingGraph,
            @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
            @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId) {
        Set<String> baIds = businessAccountLocalIdsByTargetId.get(entity.getTargetId());
        if (baIds == null) {
            return;
        }

        // currently only one BusinessAccount is discovered for Azure
        // and it's the same for AWS sub account
        if (baIds.size() == 1) {
            // add owns
            TopologyStitchingEntity businessAccount = stitchingGraph
                    .getOrCreateStitchingEntity(entitiesByLocalId.get(baIds.iterator().next()));
            businessAccount.addConnectedTo(ConnectionType.OWNS_CONNECTION, entity);
        } else {
            // this is for AWS master account, find master account and add owns
            for (String baId : baIds) {
                StitchingEntityData ba = entitiesByLocalId.get(baId);
                if (ba.getEntityDtoBuilder().getConsistsOfCount() > 0) {
                    TopologyStitchingEntity businessAccount =
                            stitchingGraph.getOrCreateStitchingEntity(ba);
                    businessAccount.addConnectedTo(ConnectionType.OWNS_CONNECTION, entity);
                    break;
                }
            }
        }
    }

    /**
     * Add the entity to be owned by related cloud service based on the pre-defined entity type
     * to cloud service mapping.
     *
     * @param entity the entity to be owned by cloud service
     * @param stitchingGraph the stitching graph that is under construction
     * @param sharedEntities the shared entities used by different cloud targets
     */
    private static void ownedByCloudService(@Nonnull final TopologyStitchingEntity entity,
            @Nonnull final TopologyStitchingGraph stitchingGraph,
            @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
        SDKProbeType probeType = entity.getProbeType();
        EntityType entityType = entity.getEntityType();

        String cloudService = CloudTopologyConverter.ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_TABLE.get(
                probeType, entityType);
        TopologyStitchingEntity cs = stitchingGraph.getOrCreateStitchingEntity(
                sharedEntities.get(probeType, EntityType.CLOUD_SERVICE).get(cloudService));

        cs.addConnectedTo(ConnectionType.OWNS_CONNECTION, entity);
    }

    /**
     * An EntityRewirer handles re-writing of the entity relationships from the old "fake cloud
     * entities" model to the new "real cloud entities" model.
     */
    private interface EntityRewirer {
        /**
         * "Rewire" a TopologyStitchingEntity as it's under construction. This must be implemented
         * by rewirers for different entity types. By default, it does nothing and return false
         * so the entity doesn't get removed.
         *
         * @param entity the {@link TopologyStitchingEntity} to rewire
         * @param entitiesByLocalId all entities discovered by the this entity's target, mapped by local id
         * @param stitchingGraph the stitching graph that is under construction
         * @param businessAccountLocalIdsByTargetId business accounts local ids for different target
         * @param sharedEntities shared entities between cloud targets, such as ComputeTiers
         * @return true, if the entity should be removed from the topology.
         */
        default boolean rewire(
                @Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            return false;
        }
    }

    // does nothing -- just uses the same behavior a non-cloud entity does.
    private static class DefaultRewirer implements EntityRewirer {}

    /**
     * Rewirer for Region. It discards any commodities bought and sold, since those commodities
     * don't have valid use cases. In new cloud model, there are only connected relationship
     * between region and other entities, but not consumes relationship.
     */
    private static class RegionRewirer implements EntityRewirer {

        @Override
        public boolean rewire(@Nonnull TopologyStitchingEntity entity,
                @Nonnull Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull TopologyStitchingGraph stitchingGraph,
                @Nonnull Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            final Map<String, StitchingEntityData> sharedRegions = sharedEntities.get(
                    entity.getProbeType(), entity.getEntityType());
            if (sharedRegions != null &&
                    !sharedRegions.containsValue(entitiesByLocalId.get(entity.getLocalId()))) {
                // remove if this Region is not included in the shared entities, which means it is a
                // duplicate Region from a different target
                return true;
            }

            // discard any commodities bought and sold
            entity.clearConsumers();
            entity.getTopologyCommoditiesSold().clear();
            entity.clearProviders();

            return false;
        }
    }

    /**
     * The AZ rewirer will remove all commodities bought and sold -- they are not relevant on AZ
     * in XL, because we expect all commodities to be bought and sold between VM/DB and Compute
     * Tiers/Storage Tiers. So we will shift all AZ buyers to the appropriate Tier entities.
     *
     * It will also need to be connected to the region(s) it is related to.
     */
    static class AvailabilityZoneRewirer implements EntityRewirer {

        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            // remove AZ for Azure for now
            if (entity.getProbeType() == SDKProbeType.AZURE) {
                return true;
            }

            // remove if this AZ is not included in the shared entities, which means it is a
            // duplicate AZ from a different target
            final Map<String, StitchingEntityData> sharedAZs = sharedEntities.get(
                    entity.getProbeType(), entity.getEntityType());
            if (sharedAZs != null &&
                    !sharedAZs.containsValue(entitiesByLocalId.get(entity.getLocalId()))) {
                return true;
            }

            // find the region to connect this AZ to
            if (entity.getCommoditiesBoughtByProvider().size() > 1) {
                logger.warn("AvailabilityZone {} has more than one ({}) providers",
                        entity.getDisplayName(), entity.getCommoditiesBoughtByProvider().size());
            }

            // region owns AZ, get region id based on AZ id, do not rely on provider relationship
            // since the providers of this AZ may have been cleared
            String regionLocalId = CloudTopologyConverter.getRegionLocalId(entity.getLocalId(),
                    entity.getProbeType());
            stitchingGraph.getOrCreateStitchingEntity(
                    sharedEntities.get(entity.getProbeType(), EntityType.REGION).get(regionLocalId))
                        .addConnectedTo(ConnectionType.OWNS_CONNECTION, entity);

            // CLOUD-TODO: I'm not sure where to transfer the commodities sold, if at all. They are
            // being discarded for now. There are "used" CPU and Memory amounts that would be nice
            // to keep, but these may be rationed off into different compute tiers, in which case
            // it'd probably be better to build those values bottom-up instead. We are not currently
            // doing that either, so I'm leaving this as a TODO item.

            // discard any commodities bought and sold
            entity.clearConsumers();
            entity.getTopologyCommoditiesSold().clear();
            entity.clearProviders();

            return false;
        }
    }

    /**
     * Rewiring of Cloud VM's. The commodities bought will be shifted from Availability Zones and
     * Storages to Compute Tiers and Storage Tiers. The VM's will also be linked to the appropriate
     * AZ.
     */
    static class VirtualMachineRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            EntityDTO.Builder entityBuilder = entity.getEntityBuilder();

            // if the VM doesn't have profileId, then it's a fake VM created for hosting
            // DatabaseServer in AWS. flag this entity for removal from the stitching graph by
            // returning "true"
            if (!entityBuilder.hasProfileId()) {
                return true;
            }

            // create a copy of the commoditiesBoughtByProvider map, so we can modify the underlying
            // object as we iterate.
            Map<TopologyStitchingEntity, List<CommodityDTO.Builder>> commoditiesBoughtByProvider
                    = new HashMap(entity.getCommoditiesBoughtByProvider());
            commoditiesBoughtByProvider.forEach((provider, commoditiesBought) -> {
                if (provider.getEntityType() == EntityType.AVAILABILITY_ZONE) {
                    // find shared AZ and connect VM to AZ
                    StitchingEntityData az = sharedEntities.get(entity.getProbeType(),
                            EntityType.AVAILABILITY_ZONE).get(provider.getLocalId());
                    TopologyStitchingEntity sharedAZ = stitchingGraph.getOrCreateStitchingEntity(az);

                    // find region based on AZ
                    String regionLocalId = CloudTopologyConverter.getRegionLocalId(
                            provider.getLocalId(), entity.getProbeType());
                    TopologyStitchingEntity region = stitchingGraph.getOrCreateStitchingEntity(
                            sharedEntities.get(entity.getProbeType(), EntityType.REGION).get(regionLocalId));

                    if (entity.getProbeType() == SDKProbeType.AWS) {
                        logger.debug("Connecting VM {} to AZ {}.", entity.getDisplayName(), sharedAZ.getDisplayName());
                        // connect to AZ (AvailabilityZone) from shared entities
                        entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, sharedAZ);
                    } else if (entity.getProbeType() == SDKProbeType.AZURE) {
                        logger.debug("Connecting VM {} to Region {}.", entity.getDisplayName(), regionLocalId);
                        // connect VM to Region
                        entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
                    }

                    // change the commodity provider to compute tier
                    // find ComputeTier based on profileId
                    StitchingEntityData computeTier = sharedEntities.get(entity.getProbeType(),
                            EntityType.COMPUTE_TIER).get(entityBuilder.getProfileId());
                    TopologyStitchingEntity ct = stitchingGraph.getOrCreateStitchingEntity(computeTier);

                    // connect CT to Region
                    ct.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);

                    // change commodities bought from AZ to CT
                    entity.putProviderCommodities(ct, commoditiesBought);
                    entity.removeProvider(provider);

                    // merge commodities sold from AZ into the compute tier.
                    // merge those commodities (with combination of commodity type and key) which
                    // exist both in VM bought commodities and PM sold commodities
                    CommoditySoldMerger commoditySoldMerger = new CommoditySoldMerger(
                            new ServiceTierSoldCommodityMergeStrategy(commoditiesBought));
                    ct.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                            provider.getTopologyCommoditiesSold(),
                            ct.getTopologyCommoditiesSold()));

                } else if (provider.getEntityType() == EntityType.STORAGE) {
                    // find the storage tier Stitching Entity we will meld this storage onto.
                    String storageTier = provider.getEntityBuilder().getStorageData().getStorageTier();
                    TopologyStitchingEntity storageTierSE = stitchingGraph.getOrCreateStitchingEntity(
                            sharedEntities.get(entity.getProbeType(), EntityType.STORAGE_TIER).get(storageTier));

                    // rewire the commodities bought
                    logger.debug("Updating commodities bought on VM {} to buy from ST {} instead"
                            + " of Storage {}", entity.getDisplayName(),
                            storageTierSE.getDisplayName(), provider.getDisplayName());
                    entity.putProviderCommodities(storageTierSE, commoditiesBought);
                    entity.removeProvider(provider);
                }
            });

            // add owns relationship, make VM to be owned by BusinessAccount
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);

            return false;
        }
    }

    /**
     * Cloud Storages each represent a combination of storage tier + availability zone. We don't
     * want to model all of those permutations directly as entities in XL, so we will merge these
     * Storages into a primary set of Storage Tier entities, decoupling Storage Tier from Availability
     * Zone. Then the Storage entities will vanish, back into the misty yonder from whence they came.
     *
     * Do not cry, for they will be heard from again. (in about ten minutes)
     *
     * We will move buyers from this storage to storage tiers when rewiring the VMs and other consumers.
     *
     */
    static class StorageRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            // A cloud storage entity represents a combination of storage tier + availability zone.
            // find the storage tier that this storage belongs in, and ensure the storage tier
            // entity contains the "connected to" relationship this storage represents.

            // Find the storage tier this storage represents
            if (!entity.getEntityBuilder().hasStorageData()) {
                // no storage data -- log an error and fall back to regular rewiring
                logger.error("Cloud storage {} has no StorageData! Will not rewire.", entity.getLocalId());
                return false;
            }
            String storageTier = entity.getEntityBuilder().getStorageData().getStorageTier();

            // find the storage tier Stitching Entity we will meld this storage onto.
            TopologyStitchingEntity storageTierSE = stitchingGraph.getOrCreateStitchingEntity(
                    sharedEntities.get(entity.getProbeType(), EntityType.STORAGE_TIER).get(storageTier));

            // find the availability zone(s) this storage is related to. (should be one)
            List<TopologyStitchingEntity> availabilityZones = entity.getTopologyCommoditiesSold().stream()
                .filter(comm -> (comm.sold.getCommodityType() == CommodityType.DSPM_ACCESS))
                .map(comm -> {
                    // we found a connected availability zone -- connect to it.
                    String azId = Converter.keyToUuid(comm.sold.getKey());
                    return stitchingGraph.getOrCreateStitchingEntity(entitiesByLocalId.get(azId));
                })
                .collect(Collectors.toList());

            // connect ST to the region
            for (TopologyStitchingEntity az : availabilityZones) {
                String regionLocalId = CloudTopologyConverter.getRegionLocalId(az.getLocalId(),
                        az.getProbeType());
                StitchingEntityData regionEntity = sharedEntities.get(entity.getProbeType(),
                        EntityType.REGION).get(regionLocalId);
                storageTierSE.addConnectedTo(ConnectionType.NORMAL_CONNECTION,
                        stitchingGraph.getOrCreateStitchingEntity(regionEntity));
            }

            // merge our commodities sold into the storage tier.
            CommoditySoldMerger commoditySoldMerger = new CommoditySoldMerger(
                    new ServiceTierSoldCommodityMergeStrategy());
            storageTierSE.setCommoditiesSold(
                    commoditySoldMerger.mergeCommoditiesSold(entity.getTopologyCommoditiesSold(),
                    storageTierSE.getTopologyCommoditiesSold()));

            // add ST to be owned by cloud service
            ownedByCloudService(storageTierSE, stitchingGraph, sharedEntities);

            // IMPORTANT: flag this entity for removal from the stitching graph by returning "true".
            return true;
        }
    }

    /**
     * Rewirer for Database from cloud targets. The relationship for AWS/Azure is as following:
     *
     * AWS:   DB -> DBServer -> VM [profileId is in DBServer]
     * Azure: DB -> DBServer -> DC [profileId is in DB]
     *
     * For AWS, we keep DB and DBServer, create ComputeTier for each distinct DBServer profile,
     * switch provider of DB from VM to ComputeTier and then remove fake VM. Also we connect DB
     * and DBServer to Region.
     *
     * For Azure, we keep DB, create ComputeTier for each distinct DB profile, switch provider of
     * DB from DBServer to ComputeTier and then remove fake DBServer. We may change it if we want
     * to keep DBServer for Azure. Also we connect DB to Region.
     */
    static class DatabaseRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            EntityDTO.Builder builder = entity.getEntityBuilder();

            if (entity.getProbeType() == SDKProbeType.AZURE) {
                // if it is from Azure, then DB has profileId
                // find DatabaseTier based on profileId
                StitchingEntityData databaseTier = sharedEntities.get(entity.getProbeType(),
                        EntityType.DATABASE_TIER).get(builder.getProfileId());
                TopologyStitchingEntity dt = stitchingGraph.getOrCreateStitchingEntity(databaseTier);

                // change provider from DBServer to ComputeTier (created from DB profile)
                Map<TopologyStitchingEntity, List<CommodityDTO.Builder>> commoditiesBoughtByProvider
                        = new HashMap(entity.getCommoditiesBoughtByProvider());

                commoditiesBoughtByProvider.forEach((provider, commoditiesBought) -> {
                    // change provider from DatabaseServer to ComputeTier
                    if (provider.getEntityType() == EntityType.DATABASE_SERVER) {
                        // find region for this DB and CT
                        // this DBServer -> Region provider relationship is only present in the
                        // Azure discovery and not in the AWS discovery, this is a special case
                        // that ideally would be handled inside each probe rather than a common
                        // method w/special cases like this.
                        provider.getProviders().stream()
                                .filter(p -> p.getEntityType() == EntityType.REGION).findFirst()
                                .ifPresent(region -> {
                                    // connect DB and CT to Region
                                    logger.debug("Connecting Database {} to Region {}.",
                                            entity.getDisplayName(), region.getDisplayName());
                                    entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
                                    logger.debug("Connecting ComputeTier {} to Region {}.",
                                            dt.getDisplayName(), region.getDisplayName());
                                    dt.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
                                });

                        // change commodities bought from DatabaseServer to DatabaseTier
                        entity.putProviderCommodities(dt, commoditiesBought);
                        entity.removeProvider(provider);
                    }
                });
            } else if (entity.getProbeType() == SDKProbeType.AWS) {
                // if from AWS, then DB is a fake one which doesn't have profileID
                // connect DB to AZ
                for (StitchingEntity prov : entity.getProviders()) {
                    if (prov.getEntityType() != EntityType.DATABASE_SERVER) {
                        continue;
                    }

                    // provider of dbServer has been switched from fakeVM to DT
                    if (!prov.getConnectedToByType().isEmpty()) {
                        entity.setConnectedTo(prov.getConnectedToByType());
                    } else {
                        // dbServer still consumes fakeVM, find AZ for this VM and connect DB to it
                        prov.getProviders().stream()
                                .filter(p -> p.getEntityType() == EntityType.VIRTUAL_MACHINE)
                                .flatMap(p -> p.getProviders().stream())
                                .filter(p -> p.getEntityType() == EntityType.AVAILABILITY_ZONE)
                                .forEach(p -> entity.addConnectedTo(
                                        ConnectionType.NORMAL_CONNECTION, p));
                    }
                }
            }

            // add owns relationship, make DB to be owned by BusinessAccount
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);

            // do not remove DB no matter whether it's from AWS or Azure
            return false;
        }
    }

    /**
     * Rewirer for DatabaseServer from AWS/Azure. The relationship is as following:
     *
     * AWS:   DB -> DBServer -> VM [profileId is in DBServer, DB is fake]
     * Azure: DB -> DBServer -> DC [profileId is in DB, DBServer is fake]
     *
     * For Azure, it just returns true since DBServer is fake and need to be removed.
     * For AWS, it finds or creates ComputeTier for each distinct DBServer profile, changes bought
     * commodities from VM to CT, merges sold commodities from VM to CT, and connect to Region.
     */
    static class DatabaseServerRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            EntityDTO.Builder builder = entity.getEntityBuilder();

            // if from Azure, then DBServer is a fake, and doesn't have profileId
            // return true so it will be discarded
            if (entity.getProbeType() == SDKProbeType.AZURE) {
                return true;
            }

            // if from AWS, then DBServer has profileId
            // first find DatabaseTier based on profileId
            StitchingEntityData databaseTier = sharedEntities.get(entity.getProbeType(),
                    EntityType.DATABASE_TIER).get(builder.getProfileId());
            TopologyStitchingEntity dt = stitchingGraph.getOrCreateStitchingEntity(databaseTier);

            Map<TopologyStitchingEntity, List<CommodityDTO.Builder>> commoditiesBoughtByProvider
                    = new HashMap(entity.getCommoditiesBoughtByProvider());

            commoditiesBoughtByProvider.forEach((provider, commoditiesBought) -> {
                // change provider from fake VM to ComputeTier (created from DBServer profile)
                if (provider.getEntityType() == EntityType.VIRTUAL_MACHINE) {
                    // connect DBServer to AZ and CT to Region
                    provider.getProviders().stream()
                        // this filter will work because we are not rewiring the fake database VMs
                        // we would not normally expect an AZ provider for a VM to exist since
                        // provider may have been switched to CT due to other rewiring rules
                        .filter(p -> p.getEntityType() == EntityType.AVAILABILITY_ZONE)
                        .map(p -> (TopologyStitchingEntity)p)
                        .findFirst()
                        .ifPresent(p -> {
                            // connect DBS to AZ
                            StitchingEntityData stitchingEntityData = sharedEntities.get(
                                    p.getProbeType(), p.getEntityType()).get(p.getLocalId());
                            TopologyStitchingEntity az = stitchingGraph
                                    .getOrCreateStitchingEntity(stitchingEntityData);

                            logger.debug("Connecting DatabaseServer {} to AZ {}.",
                                    entity.getDisplayName(), az.getDisplayName());
                            entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, az);

                            // connect DT to Region
                            String regionLocalId = CloudTopologyConverter.getRegionLocalId(
                                    az.getLocalId(), az.getProbeType());
                            TopologyStitchingEntity region = stitchingGraph.getOrCreateStitchingEntity(
                                    sharedEntities.get(entity.getProbeType(), EntityType.REGION).get(regionLocalId));
                            logger.debug("Connecting DatabaseTier {} to Region {}.",
                                    dt.getDisplayName(), region.getDisplayName());
                            dt.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
                        });

                    // change commodities bought from VM to DT
                    entity.putProviderCommodities(dt, commoditiesBought);
                    entity.removeProvider(provider);

                    // merge commodities sold from VM to DatabaseTier (DatabaseServer)
                    CommoditySoldMerger commoditySoldMerger = new CommoditySoldMerger(
                            new ServiceTierSoldCommodityMergeStrategy(commoditiesBought));
                    dt.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                            provider.getTopologyCommoditiesSold(), dt.getTopologyCommoditiesSold()));
                }
            });

            // add owns relationship, make VM to be owned by BusinessAccount
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);

            return false;
        }
    }

    /**
     * The compute tier rewirer. COMPUTE_TIER comes from vm profile in AWS and Azure.
     *
     * Connect ComputeTier to Region based on license info in profile dto. For AWS, those compute
     * tiers which are referenced in VM dto, are connected to AZ in {@link VirtualMachineRewirer}.
     */
    static class ComputeTierRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            // CLOUD-TODO: figure out how to store info from EntityProfileDTO to EntityDTO
            // this is a general task for how to store entity specific information in XL

            // find shared ComputeTier
            StitchingEntityData computeTierData = sharedEntities.get(entity.getProbeType(),
                    EntityType.COMPUTE_TIER).get(entity.getLocalId());
            TopologyStitchingEntity computeTier = stitchingGraph.getOrCreateStitchingEntity(computeTierData);

            EntityProfileDTO profileDTO = computeTierData.getEntityProfileDto();

            // connect CT to Region
            profileDTO.getVmProfileDTO().getLicenseList().forEach(licenseMapEntry -> {
                StitchingEntityData regionEntityData = sharedEntities.get(entity.getProbeType(),
                        EntityType.REGION).get(licenseMapEntry.getRegion());
                TopologyStitchingEntity region = stitchingGraph.getOrCreateStitchingEntity(regionEntityData);
                computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
            });

            // connect CT to ST
            // CT for VM profile can be connected ST, since it contains diskType
            String diskType = profileDTO.getVmProfileDTO().getDiskType();
            // aws vm profile
            if (entity.getProbeType() == SDKProbeType.AWS) {
                // AWS: diskType can be ssd, hdd, ebs
                if (diskType.equals("ebs")) {
                    // connect to all AWS ebs StorageTiers
                    sharedEntities.get(SDKProbeType.AWS, EntityType.STORAGE_TIER)
                            .values().forEach(storageTier -> {
                        TopologyStitchingEntity st = stitchingGraph.getOrCreateStitchingEntity(storageTier);
                        computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, st);
                    });
                } else {
                    // connect CT to ST
                    TopologyStitchingEntity st = stitchingGraph.getOrCreateStitchingEntity(
                            sharedEntities.get(SDKProbeType.AWS, EntityType.STORAGE_TIER)
                                    .get(diskType.toUpperCase()));
                    computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, st);
                }
            } else if (entity.getProbeType() == SDKProbeType.AZURE) {
                // Azure: diskType can be SSD, HDD
                // only STANDARD supports HDD, PREMIUM supports both SSD and HDD
                // connect SSD to managed_standard, managed_premium, unmanaged_standard, unmanaged_premium
                // connect HDD to managed_standard, unmanaged_standard,
                // cloud-todo: how to make it not hardcoded?
                sharedEntities.get(SDKProbeType.AZURE, EntityType.STORAGE_TIER).values()
                        .forEach(storageTier -> {
                    if ("SSD".equalsIgnoreCase(diskType) || ("HDD".equalsIgnoreCase(diskType) &&
                                    storageTier.getEntityDtoBuilder().getDisplayName().contains("STANDARD"))) {
                        TopologyStitchingEntity st = stitchingGraph.getOrCreateStitchingEntity(storageTier);
                        computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, st);
                        //todo: can it be bidirectional?
                        st.addConnectedTo(ConnectionType.NORMAL_CONNECTION, computeTier);
                    }
                });
            }

            // add sold commodities for ComputeTier
            // todo: currently we use commodityProfile defined in entityProfile to create commodities
            // for CT, some commodity only has consumed defined, not capacity
            // figure out better ways to create commodities and which types should be there
            List<CommoditySold> soldCommoditiesFromCommodityProfiles = profileDTO
                    .getCommodityProfileList().stream().map(commodityProfileDTO ->
                            new CommoditySold(CommodityDTO.newBuilder()
                                    .setCommodityType(commodityProfileDTO.getCommodityType())
                                    .setCapacity(commodityProfileDTO.getCapacity()), null)
            ).collect(Collectors.toList());

            // merge onto existing sold commodities which are created in other rewirer
            CommoditySoldMerger commoditySoldMerger = new CommoditySoldMerger(
                    new ServiceTierSoldCommodityMergeStrategy());
            computeTier.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                    soldCommoditiesFromCommodityProfiles,
                    computeTier.getTopologyCommoditiesSold()));

            // add CT to be owned by cloud service
            ownedByCloudService(computeTier, stitchingGraph, sharedEntities);

            return false;
        }
    }

    /**
     * The database tier rewirer. DatabaseTier comes from profile:
     *     AWS:   db server profile
     *     Azure: db profile
     *
     * Connect DatabaseTier to Region based on license info in profile dto.
     */
    static class DatabaseTierRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            // CLOUD-TODO: figure out how to store info from EntityProfileDTO to EntityDTO
            // this is a general task for how to store entity specific information in XL

            // find shared DatabaseTier
            StitchingEntityData databaseTierData = sharedEntities.get(entity.getProbeType(),
                    EntityType.DATABASE_TIER).get(entity.getLocalId());
            TopologyStitchingEntity databaseTier = stitchingGraph.getOrCreateStitchingEntity(
                    databaseTierData);

            EntityProfileDTO profileDTO = databaseTierData.getEntityProfileDto();

            // connect DT to Region
            profileDTO.getDbProfileDTO().getLicenseList().forEach(licenseMapEntry -> {
                StitchingEntityData regionEntityData = sharedEntities.get(entity.getProbeType(),
                        EntityType.REGION).get(licenseMapEntry.getRegion());
                TopologyStitchingEntity region = stitchingGraph.getOrCreateStitchingEntity(regionEntityData);
                databaseTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, region);
            });

            // add sold commodities for DatabaseTier
            // todo: currently we use commodityProfile defined in entityProfile to create commodities
            // for CT, some commodity only has consumed defined, not capacity
            // figure out better ways to create commodities and which types should be there
            List<CommoditySold> soldCommoditiesFromCommodityProfiles = profileDTO
                    .getCommodityProfileList().stream().map(commodityProfileDTO ->
                            new CommoditySold(CommodityDTO.newBuilder()
                                    .setCommodityType(commodityProfileDTO.getCommodityType())
                                    .setCapacity(commodityProfileDTO.getCapacity()), null)
                    ).collect(Collectors.toList());

            // merge onto existing sold commodities which are created in other rewirer
            CommoditySoldMerger commoditySoldMerger = new CommoditySoldMerger(
                    new ServiceTierSoldCommodityMergeStrategy());
            databaseTier.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                    soldCommoditiesFromCommodityProfiles,
                    databaseTier.getTopologyCommoditiesSold()));

            // add DT to be owned by cloud service
            ownedByCloudService(databaseTier, stitchingGraph, sharedEntities);

            return false;
        }
    }

    /**
     * Rewirer for BusinessAccount. For AWS: set up "owns" between master account & sub account
     */
    static class BusinessAccountRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            EntityDTO.Builder builder = entity.getEntityBuilder();

            if (entity.getProbeType() == SDKProbeType.AWS) {
                builder.getConsistsOfList().forEach(subAccountId -> {
                    TopologyStitchingEntity subAccount = stitchingGraph
                            .getOrCreateStitchingEntity(entitiesByLocalId.get(subAccountId));
                    entity.addConnectedTo(ConnectionType.OWNS_CONNECTION, subAccount);
                });
            }
            return false;
        }
    }

    /**
     * Rewirer for LoadBalancer. It adds LB to be owned by BusinessAccount.
     */
    static class LoadBalancerRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);
            return false;
        }
    }

    /**
     * Rewirer for Application. It adds App to be owned by BusinessAccount.
     */
    static class ApplicationRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);
            return false;
        }
    }

    /**
     * Rewirer for VirtualApplication. It adds vApp to be owned by BusinessAccount.
     */
    static class VirtualApplicationRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            ownedByBusinessAccount(entity, stitchingGraph, businessAccountLocalIdsByTargetId, entitiesByLocalId);
            return false;
        }
    }

    /**
     * Rewirer for DiskArray. DA is a fake entity and should be discarded. No commodities or
     * relationship are needed.
     */
    static class DiskArrayRewirer implements EntityRewirer {
        @Override
        public boolean rewire(@Nonnull final TopologyStitchingEntity entity,
                @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
                @Nonnull final TopologyStitchingGraph stitchingGraph,
                @Nonnull final Map<Long, Set<String>> businessAccountLocalIdsByTargetId,
                @Nonnull final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities) {
            return true;
        }
    }
}
