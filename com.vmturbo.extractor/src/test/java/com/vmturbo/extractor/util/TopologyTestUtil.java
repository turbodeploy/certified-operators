package com.vmturbo.extractor.util;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.commons.lang3.ObjectUtils;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Discovered;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.SupportedCustomerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CronJobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CustomControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DaemonSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DeploymentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.JobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.LogicalPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ReplicaSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ReplicationControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StatefulSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskGroupData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskRole;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.RawCapacity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StoragePolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageRedundancyMethod;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Utility class for tests involving topology processing.
 */
public class TopologyTestUtil {
    private TopologyTestUtil() {
    }

    private static final AtomicLong nextId = new AtomicLong(1L);

    /**
     * Create a {@link TopologyInfo} for a given creation timestamp.
     *
     * @param time creation time for topology, or null to use current time
     * @return new topology info
     */
    public static TopologyInfo mkRealtimeTopologyInfo(Long time) {
        return TopologyInfo.newBuilder()
                .setCreationTime(time != null ? time : System.currentTimeMillis())
                .setTopologyContextId((ComponentUtils.REALTIME_TOPOLOGY_CONTEXT))
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyId(nextId.getAndIncrement())
                .build();
    }

    /**
     * Create a topology for testing, comprising a {@link TopologyInfo} instance and a list of
     * entities.
     */
    public static class TestTopology {
        private final TopologyInfo topologyInfo;
        private final TopologyEntityDTO[] entities;

        /**
         * Create a new instance.
         *
         * @param topologyInfo topology info
         * @param entities     entities appearing in the topology
         */
        public TestTopology(TopologyInfo topologyInfo, TopologyEntityDTO... entities) {
            this.topologyInfo = topologyInfo;
            this.entities = entities;
        }
    }

    /**
     * Method to act like a {@link RemoteIterator} to feed entities from a topology to a topology
     * listener.
     *
     * @param topology the topology to be sent
     * @param listener listener to receive topology
     */
    public static void feedTopologyToListener(TestTopology topology, EntitiesListener listener) {
        listener.onTopologyNotification(topology.topologyInfo, new RemoteIterator<DataSegment>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < topology.entities.length;
            }

            @Nonnull
            @Override
            public Collection<DataSegment> nextChunk() throws InterruptedException, TimeoutException, CommunicationException {
                if (hasNext()) {
                    return Collections.singletonList(
                            DataSegment.newBuilder()
                                    .setEntity(topology.entities[i++])
                                    .build());
                } else {
                    throw new NoSuchElementException();
                }
            }
        }, Mockito.mock(SpanContext.class));
    }

    /**
     * Create an {@link TopologyEntityDTO} for a given entity type.
     *
     * <p>The created entity will have a {@link TypeSpecificInfo} value of an appropriate type,
     * with default state.</p>
     *
     * @param type desired entity type
     * @return new entity structure
     */
    public static TopologyEntityDTO mkEntity(EntityType type) {
        final long oid = nextId.getAndIncrement();
        Optional<TypeSpecificInfo> defaultTSI = getDefaultTSI(type);
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(type.getNumber())
                .setDisplayName(String.format("%s #%d", type, oid));
        defaultTSI.ifPresent(builder::setTypeSpecificInfo);
        return builder.build();
    }

    private static Optional<TypeSpecificInfo> getDefaultTSI(EntityType type) {
        TypeSpecificInfo.Builder builder = TypeSpecificInfo.newBuilder();
        switch (type) {
            case APPLICATION:
                builder.setApplication(fillApplication(ApplicationInfo.newBuilder()));
                break;
            case BUSINESS_ACCOUNT:
                builder.setBusinessAccount(fillBusinessAccount(BusinessAccountInfo.newBuilder()));
                break;
            case BUSINESS_USER:
                builder.setBusinessUser(fillBusinessUser(BusinessUserInfo.newBuilder()));
                break;
            case LOGICAL_POOL:
                builder.setLogicalPool(fillLogicalPool(LogicalPoolInfo.newBuilder()));
                break;
            case REGION:
                builder.setRegion(fillRegion(RegionInfo.newBuilder()));
                break;
            case COMPUTE_TIER:
                builder.setComputeTier(fillComputeTier(ComputeTierInfo.newBuilder()));
                break;
            case DATABASE:
                builder.setDatabase(fillDatabase(DatabaseInfo.newBuilder()));
                break;
            case DATABASE_SERVER_TIER:
                builder.setDatabaseServerTier(fillDatabaseServerTier(DatabaseServerTierInfo.newBuilder()));
                break;
            case DATABASE_TIER:
                builder.setDatabaseTier(fillDatabaseTier(DatabaseTierInfo.newBuilder()));
                break;
            case DESKTOP_POOL:
                builder.setDesktopPool(fillDesktopPool(DesktopPoolInfo.newBuilder()));
                break;
            case DISK_ARRAY:
                builder.setDiskArray(fillDiskArray(DiskArrayInfo.newBuilder()));
                break;
            case PHYSICAL_MACHINE:
                builder.setPhysicalMachine(fillPhysicalMachine(PhysicalMachineInfo.newBuilder()));
                break;
            case STORAGE:
                builder.setStorage(fillStorage(StorageInfo.newBuilder()));
                break;
            case STORAGE_CONTROLLER:
                builder.setStorageController(fillStorageController(StorageControllerInfo.newBuilder()));
                break;
            case VIRTUAL_MACHINE:
                builder.setVirtualMachine(fillVirtualMachine(VirtualMachineInfo.newBuilder()));
                break;
            case VIRTUAL_VOLUME:
                builder.setVirtualVolume(fillVirtualVolume(VirtualVolumeInfo.newBuilder()));
                break;
            case WORKLOAD_CONTROLLER:
                builder.setWorkloadController(fillWorkloadController(WorkloadControllerInfo.newBuilder()));
                break;
            default:
                // no type specific info for this entity type
                return Optional.empty();
        }
        return Optional.of(builder.build());
    }

    private static ApplicationInfo fillApplication(ApplicationInfo.Builder builder) {
        return builder.setIpAddress(IpAddress.newBuilder()
                .setIpAddress("10.10.10.1")
                .setIsElastic(true))
                .build();
    }

    private static BusinessAccountInfo fillBusinessAccount(BusinessAccountInfo.Builder builder) {
        return builder.setAccountId("acct")
                .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.OFFER_ID)
                        .setIdentifierValue("xxx")
                        .build())
                .setAssociatedTargetId(1L)
                .setRiSupported(true)
                .build();
    }

    private static BusinessUserInfo fillBusinessUser(BusinessUserInfo.Builder builder) {
        return builder.putVmOidToSessionDuration(1L, 100L)
                .build();
    }

    private static LogicalPoolInfo fillLogicalPool(LogicalPoolInfo.Builder builder) {
        return builder.setDiskTypeInfo(
                DiskTypeInfo.newBuilder()
                        .setNum10KDisks(10)
                        .setNum15KDisks(10)
                        .setNum7200Disks(10)
                        .setNumSsd(10)
                        .setNumVSeriesDisks(10)
                        .build())
                .build();
    }

    private static RegionInfo fillRegion(RegionInfo.Builder builder) {
        return builder.setGeoData(
                GeoDataInfo.newBuilder()
                        .setLatitude(100.0)
                        .setLongitude(100.0)
                        .build())
                .build();
    }

    private static ComputeTierInfo fillComputeTier(ComputeTierInfo.Builder builder) {
        return builder.setBurstableCPU(true)
                .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_DISABLED)
                .setFamily("xxx")
                .setInstanceDiskSizeGb(1000)
                .setInstanceDiskType(InstanceDiskType.HDD)
                .setNumCores(12)
                .setNumCoupons(1)
                .setNumInstanceDisks(3)
                .setQuotaFamily("xxx")
                .setSupportedCustomerInfo(
                        SupportedCustomerInfo.newBuilder()
                                .addSupportedArchitectures(Architecture.ARM_64)
                                .addSupportedVirtualizationTypes(VirtualizationType.HVM)
                                .setSupportsOnlyEnaVms(true)
                                .setSupportsOnlyNVMeVms(false)
                                .build())
                .build();
    }

    private static DatabaseInfo fillDatabase(final DatabaseInfo.Builder builder) {
        return builder.setDeploymentType(DeploymentType.MULTI_AZ)
                .setEdition(DatabaseEdition.ENTERPRISE)
                .setEngine(DatabaseEngine.MARIADB)
                .setLicenseModel(LicenseModel.BRING_YOUR_OWN_LICENSE)
                .setVersion("10.6.4")
                .build();
    }

    private static DatabaseServerTierInfo fillDatabaseServerTier(final DatabaseServerTierInfo.Builder builder) {
        return builder.setFamily("family")
                .build();
    }

    private static DatabaseTierInfo fillDatabaseTier(final DatabaseTierInfo.Builder builder) {
        return builder.setFamily("family")
                .setEdition(DatabaseEdition.ENTERPRISE.name())
                .build();
    }

    private static DesktopPoolInfo fillDesktopPool(DesktopPoolInfo.Builder builder) {
        return builder.setAssignmentType(DesktopPoolAssignmentType.DYNAMIC)
                .setCloneType(DesktopPoolCloneType.FULL)
                .setProvisionType(DesktopPoolProvisionType.ON_DEMAND)
                .setTemplateReferenceId(10L)
                .build();
    }

    private static DiskArrayInfo fillDiskArray(DiskArrayInfo.Builder builder) {
        return builder.setDiskTypeInfo(
                DiskTypeInfo.newBuilder()
                        .setNum10KDisks(10)
                        .setNum15KDisks(10)
                        .setNum7200Disks(10)
                        .setNumSsd(10)
                        .setNumVSeriesDisks(10)
                        .build())
                .build();
    }

    private static PhysicalMachineInfo fillPhysicalMachine(PhysicalMachineInfo.Builder builder) {
        return builder.setCpuCoreMhz(2600)
                .setCpuModel("XXX")
                .addDiskGroup(DiskGroupData.newBuilder()
                        .addDisk(DiskData.newBuilder()
                                .setCapacity(100000000000L)
                                .setRole(DiskRole.ROLE_CACHE)
                                .build())
                        .build())
                .setModel("zcvzxv")
                .setNumCpus(12)
                .setNumCpuSockets(20)
                .setTimezone("UTC")
                .setVendor("IBM")
                .build();
    }

    private static StorageInfo fillStorage(StorageInfo.Builder builder) {
        return builder.addExternalName("foo")
                .setIsLocal(true)
                .setPolicy(StoragePolicy.newBuilder()
                        .setFailuresToTolerate(10)
                        .setRedundancy(StorageRedundancyMethod.RAID0)
                        .setSpaceReservationPct(10)
                        .build())
                .setRawCapacity(RawCapacity.newBuilder()
                        .setCapacity(100000000)
                        .setFree(100000000)
                        .setUncommitted(100000)
                        .build())
                .setStorageType(StorageType.CIFS_SMB)
                .build();
    }

    private static StorageControllerInfo fillStorageController(StorageControllerInfo.Builder builder) {
        return builder.setDiskTypeInfo(
                DiskTypeInfo.newBuilder()
                        .setNum10KDisks(10)
                        .setNum15KDisks(10)
                        .setNum7200Disks(10)
                        .setNumSsd(10)
                        .setNumVSeriesDisks(10)
                        .build())
                .build();
    }

    private static VirtualMachineInfo fillVirtualMachine(VirtualMachineInfo.Builder builder) {
        return builder.setArchitecture(Architecture.ARM_64)
                .setBillingType(VMBillingType.BIDDING)
                .addConnectedNetworks("net1")
                .setDriverInfo(
                        DriverInfo.newBuilder()
                                .setHasEnaDriver(true)
                                .setHasNvmeDriver(false)
                                .build())
                .setDynamicMemory(true)
                .setGuestOsInfo(OS.newBuilder()
                        .setGuestOsName("Ubuntu")
                        .setGuestOsType(OSType.LINUX)
                        .setGuestOsName("walrus")
                        .build())
                .addIpAddresses(IpAddress.newBuilder()
                        .setIpAddress("10.10.10.1")
                        .setIsElastic(false)
                        .build())
                .setLicenseModel(EntityDTO.LicenseModel.AHUB)
                .setLocks("xyzzy")
                .setNumCpus(12)
                .setTenancy(Tenancy.DEDICATED)
                .setVirtualizationType(VirtualizationType.HVM)
                .build();
    }

    private static VirtualVolumeInfo fillVirtualVolume(VirtualVolumeInfo.Builder builder) {
        return builder.setAttachmentState(AttachmentState.ATTACHED)
                .setEncryption(true)
                .addFiles(VirtualVolumeFileDescriptor.newBuilder()
                        .setPath("/blah/blah")
                        .setSizeKb(1000)
                        .setType(VirtualMachineFileType.CONFIGURATION)
                        .setModificationTimeMs(12341242313412L)
                        .addLinkedPaths("/foo/bar")
                        .build())
                .setIsEphemeral(true)
                .setRedundancyType(RedundancyType.GRS)
                .setSnapshotId("dasfasf")
                .build();
    }

    private static WorkloadControllerInfo fillWorkloadController(WorkloadControllerInfo.Builder builder) {
        return builder.setCronJobInfo(
                CronJobInfo.newBuilder()
                        .build())
                .setCustomControllerInfo(CustomControllerInfo.newBuilder()
                        .setCustomControllerType("secret")
                        .build())
                .setDaemonSetInfo(DaemonSetInfo.newBuilder()
                        .build())
                .setDeploymentInfo(DeploymentInfo.newBuilder()
                        .build())
                .setJobInfo(JobInfo.newBuilder()
                        .build())
                .setReplicaSetInfo(ReplicaSetInfo.newBuilder()
                        .build())
                .setReplicationControllerInfo(ReplicationControllerInfo.newBuilder()
                        .build())
                .setStatefulSetInfo(StatefulSetInfo.newBuilder()
                        .build())
                .build();
    }

    /**
     * Create a list of {@link CommoditySoldDTO}s.
     *
     * <p>Values from which sold commodities are bulit come in the form of quartets (4-tuples)
     * of commodity type, commodity key, used value, and capacity value, in that order.</p>
     *
     * @param commodities sold commodity parameters
     * @return the new sold commodity structure list
     */
    public static List<CommoditySoldDTO> soldCommodities(
            Quartet<CommodityDTO.CommodityType, String, Double, Double>... commodities) {
        return Arrays.stream(commodities)
                .map(t -> CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(t.getValue0().getNumber())
                                .setKey(ObjectUtils.firstNonNull(t.getValue1(), ""))
                                .build())
                        .setUsed(t.getValue2())
                        .setCapacity(t.getValue3())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Construct a sold commodity, including percentile historical utilization.
     *
     * @param commodityType the type of commodity to construct
     * @param used the amount used
     * @param capacity the capacity
     * @param percentileUtilization the percentile historical utilization, as a percentage
     * @return the constructed commodity sold
     */
    public static CommoditySoldDTO soldCommodityWithPercentile(CommodityDTO.CommodityType commodityType,
                                                 double used, double capacity, double percentileUtilization) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(commodityType.getNumber()))
            .setUsed(used)
            .setCapacity(capacity)
            .setHistoricalUsed(HistoricalValues.newBuilder()
                .setPercentile(percentileUtilization))
            .build();
    }

    /**
     * Construct a sold commodity, including weighted historical utilization.
     *
     * @param commodityType the type of commodity to construct
     * @param used the amount used
     * @param capacity the capacity
     * @param historicalUtilization the historical utilization, as a percentage
     * @return the constructed commodity sold
     */
    public static CommoditySoldDTO soldCommodityWithHistoricalUtilization(CommodityDTO.CommodityType commodityType,
                                                 double used, double capacity, double historicalUtilization) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(commodityType.getNumber()))
            .setUsed(used)
            .setCapacity(capacity)
            .setHistoricalUsed(HistoricalValues.newBuilder()
                .setHistUtilization(historicalUtilization))
            .build();
    }

    /**
     * Create a {@link CommoditiesBoughtFromProvider}.
     *
     * <p>Multiple commodities can be specified, each in the form of a a (type, key, used)
     * triple</p>
     *
     * @param provider    provider entity
     * @param commodities commodities consumed from teh provider
     * @return {@link CommoditiesBoughtFromProvider}
     */
    public static CommoditiesBoughtFromProvider boughtCommoditiesFromProvider(
            TopologyEntityDTO provider,
            Triplet<CommodityDTO.CommodityType, String, Double>... commodities) {
        final Builder builder = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(provider.getOid())
                .setProviderEntityType(provider.getEntityType());
        Arrays.stream(commodities)
                .map(comm -> CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(comm.getValue0().getNumber())
                                .setKey(ObjectUtils.firstNonNull(comm.getValue1(), "")))
                        .setUsed(comm.getValue2())
                        .build())
                .forEach(builder::addCommodityBought);
        return builder.build();
    }

    /**
     * Create a static group of a given type and with a given list of member oids.
     *
     * @param groupType group type
     * @param members   list of member oids
     * @return the newly constructed group
     */
    public static Grouping mkGroup(GroupType groupType, List<Long> members) {
        final long oid = nextId.getAndIncrement();
        return Grouping.newBuilder()
                .setId(oid)
                .setOrigin(Origin.newBuilder()
                        .setDiscovered(Discovered.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(String.format("%s #%d", groupType, oid))
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.PHYSICAL_MACHINE_VALUE))
                                        .addAllMembers(members))))
                .build();
    }

    /**
     * Create an on-prem volume entity.
     *
     * @param files           files to be listed in the volume
     * @param attachmentState attachment type for listed files
     * @param storageId       id of connected storage entity
     * @return the new virtual-volume entity
     */
    public static TopologyEntityDTO onPremVolume(List<VirtualVolumeFileDescriptor> files,
            AttachmentState attachmentState, long storageId) {
        return mkEntity(VIRTUAL_VOLUME).toBuilder()
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setAttachmentState(attachmentState)
                                .addAllFiles(files)))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(storageId)
                        .setConnectedEntityType(STORAGE.getNumber()))
                .build();
    }

    /**
     * Create a cloud volume entity.
     *
     * @param files           files to be listed in the volume
     * @param attachmentState attachment type for listed files
     * @param storageTierId   storage tier entity id
     * @return the new virtual-volume entity
     */
    public static TopologyEntityDTO cloudVolume(List<VirtualVolumeFileDescriptor> files,
            AttachmentState attachmentState, long storageTierId) {
        return mkEntity(VIRTUAL_VOLUME).toBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setAttachmentState(attachmentState)
                                .addAllFiles(files)))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(storageTierId)
                        .setProviderEntityType(STORAGE_TIER_VALUE)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setUsed(20)
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))))
                .build();
    }

    /**
     * Create a file entry for a virtual volume entity.
     *
     * @param fileName             filename
     * @param fileType             file type
     * @param fileSize             file size
     * @param lastModificationTime last modification time
     * @return new file descriptor
     */
    public static VirtualVolumeFileDescriptor file(String fileName, VirtualMachineFileType fileType,
            long fileSize, long lastModificationTime) {
        return VirtualVolumeFileDescriptor.newBuilder()
                .setPath(fileName)
                .setSizeKb(fileSize)
                .setModificationTimeMs(lastModificationTime)
                .setType(fileType)
                .build();
    }
}
