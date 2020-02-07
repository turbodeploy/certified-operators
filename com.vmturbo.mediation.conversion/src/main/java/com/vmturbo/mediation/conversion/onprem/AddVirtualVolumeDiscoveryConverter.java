package com.vmturbo.mediation.conversion.onprem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StorageFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VirtualMachineFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * A utility class that takes the original {@link DiscoveryResponse} from hypervisor probe
 * (vc, hyperv and vmm) or vc storage browsing probe, and convert it to the format needed for the
 * "Lock VMs to Datastores" feature.
 *
 * Generally, it creates a VirtualVolume entity for each combination of VirtualMachine and Storage,
 * and sets up the relationship so that VM is connected to VirtualVolume, and VirtualVolume is
 * connected to Storage.
 *
 * If it's vc storage browsing probe, it also performs following operations:
 * We keep the file information that is originally in the virtual_machine_data in a VirtualVolume
 * that is connected to the VM and the Storage.  We also create a VirtualVolume associated with
 * each Storage to hold all the files in the storage_data.  We then remove all the files associated
 * with VMs from this "wasted storage" volume.  Note that in the case of shared storage, some of
 * these wasted files are not really wasted (since VMs on another target consume them).  This is
 * handled in a pre stitching operation which will merge the wasted storage volumes common to
 * multiple targets by taking the intersection of all the files on the various wasted storage
 * volumes.
 */
public class AddVirtualVolumeDiscoveryConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A Builder for building the new discovery response that contains the VirtualVolumes.
     */
    private final DiscoveryResponse.Builder discoveryResponseBuilder;

    /**
     * A table that maps vm id and storage id to the list of files that the vm has on that storage.
     */
    private final Table<String, String, List<VirtualMachineFileDescriptor>> vmToStorageFiles =
            HashBasedTable.create();

    /**
     * A map from storage ID to the list of files on that storage.
     */
    private final Map<String, List<StorageFileDescriptor>> storageIdToFiles = Maps.newHashMap();

    /**
     * A map from storage ID to a set of file paths on that storage that are consumed by a VM.
     */
    private final Map<String, Set<String>> storageIdToUsedFilePaths = Maps.newHashMap();

    /**
     * A map from VM id to its EntityDTO.
     */
    private final Map<String, EntityDTO.Builder> vmDtoById = Maps.newHashMap();

    /**
     * The collection of all network display names
     */
    private final Map<String, String> networkIds = Maps.newHashMap();

    /**
     * A map from storage id to its EntityDTO.
     */
    private final Map<String, EntityDTO.Builder> storageDtoById = Maps.newHashMap();

    /**
     * A table that maps vm id and storage id to the corresponding commodityBought that the vm buys
     * from that storage.
     */
    private final Table<String, String, CommodityBought.Builder> vmToStorageCommodityBought =
        HashBasedTable.create();

    /**
     * A prefix we attach to the beginning of the VirtualVolume ID.
     */
    private static final String VIRTUAL_VOLUME_ID_PREFIX = "Vol-";

    /**
     * A suffix we add to the VirtualVolume ID for those virtual volumes representing wasted files.
     */
    @VisibleForTesting
    public static final String WASTED_VOLUMES_SUFFIX = "wastedFiles";

    private static final String ID_SEPARATOR = "-";

    /**
     * The prefix that the VimStorageBrowsing probe attaches to VM and Storage IDs in the ID field
     * of the EntityDTO but NOT in the provider id field or in the storageId field of the
     * VirtualMachineFileDescriptor.
     */
    @VisibleForTesting
    public static final String STORAGE_BROWSING_ID_PREFIX = "dsb-";

    /**
     * boolean flag indicating whether this converter is used for vc storage browsing probe.
     */
    private final boolean isStorageBrowsing;

    public AddVirtualVolumeDiscoveryConverter(@Nonnull DiscoveryResponse discoveryResponse,
                                              boolean isStorageBrowsing) {
        this.discoveryResponseBuilder = discoveryResponse.toBuilder();
        this.isStorageBrowsing = isStorageBrowsing;
    }

    /**
     * Convert old discovery response to new discovery response consistent with the XL model.
     *
     * @return new discovery response
     */
    public DiscoveryResponse convert() {
        // pre process discovery response to create data structures with relationships among files,
        // virtual machines and storages, which will be the basis for virtual volume creation
        preProcess();

        // create volumes between VMs and Storages
        List<EntityDTO> volumes = createVirtualVolumesFromVms();
        if (this.isStorageBrowsing) {
            // if it's vc storage browsing probe, also create volumes just connected to storages
            volumes.addAll(createVirtualVolumesFromStorages());
        }

        // VM DTOs will now have layeredOver relationship with Volume which topology-processor
        // will convert to connectedTo/connectedFrom and VirtualVolumes will have layeredOver
        // relationships with Storages.
        discoveryResponseBuilder.addAllEntityDTO(volumes);
        return discoveryResponseBuilder.build();
    }

    /**
     * <p>Pre process discovery response to create data structures with relationships among files,
     * virtual machines and storage. These will be the basis for virtual volume creation.</p>
     *
     * <p>Also: populate the {@code connected_networks} field for VMs using their layered-over
     * relationship.</p>
     */
    private void preProcess() {
        discoveryResponseBuilder.getEntityDTOBuilderList().forEach(entityBuilder -> {
            final String entityId = entityBuilder.getId();
            if (entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE) {
                // mapping from vm id to vm dto builder
                vmDtoById.put(entityId, entityBuilder);

                // populate the Table of VMFileDescriptors indexed by VM id and storage id
                if (this.isStorageBrowsing) {
                    // for storage browsing, go through all files list since it contains storage id
                    entityBuilder.getVirtualMachineData().getFileList().stream()
                        .filter(VirtualMachineFileDescriptor::hasStorageId)
                        .forEach(fileDescriptor -> {
                            final String storageId = STORAGE_BROWSING_ID_PREFIX
                                + fileDescriptor.getStorageId();
                            vmToStorageFiles.row(entityId)
                                .computeIfAbsent(storageId, s -> Lists.newArrayList())
                                .add(fileDescriptor);
                            storageIdToUsedFilePaths.computeIfAbsent(storageId,
                                s -> Sets.newHashSet())
                                .add(fileDescriptor.getPath());
                        });
                    // convert all provider id in commodity bought to be prefixed with dsb
                    convertCommodityBoughtProviderId(entityBuilder);

                    // Remove the files from the VM. Lots of files blows up the size of the entity.
                    // Now that we created the vm file table we don't need to know the files from
                    // the VM directly. We will create volumes for each file.
                    entityBuilder.getVirtualMachineDataBuilder().clearFile();
                } else {
                    // for hypervisor probes (vc, hyperV, vmm), go though commodities bought list
                    // since vm data doesn't contain files
                    entityBuilder.getCommoditiesBoughtBuilderList().stream()
                        .filter(commodityBought ->
                            commodityBought.getProviderType() == EntityType.STORAGE)
                        .forEach(commodityBought -> {
                            vmToStorageFiles.put(entityId, commodityBought.getProviderId(),
                                Collections.emptyList());
                            // put the commodityBought into a table indexed by vm-storage
                            vmToStorageCommodityBought.put(entityId,
                                commodityBought.getProviderId(), commodityBought);
                        });
                }
            } else if (entityBuilder.getEntityType() == EntityType.STORAGE) {
                storageDtoById.put(entityId, entityBuilder);
            } else if (entityBuilder.getEntityType() == EntityType.NETWORK) {
                networkIds.put(entityBuilder.getId(), entityBuilder.getDisplayName());
            }

            // clear "layeredOver" for all entities except those of VMs
            if (entityBuilder.getEntityType() != EntityType.VIRTUAL_MACHINE) {
                entityBuilder.clearLayeredOver();
            }
        });

        // if it's storage browsing probe, we should also populate the map of storage ID
        // to the files that exist on that storage but not on any VMs
        if (this.isStorageBrowsing) {
            storageDtoById.values()
                .stream()
                .filter(Builder::hasStorageData)
                .filter(builder -> builder.getStorageData().getFileCount() > 0)
                .forEach(builder -> {
                    storageIdToFiles.put(builder.getId(),
                        builder.getStorageData().getFileList()
                            .stream()
                            .filter(file -> !storageIdToUsedFilePaths
                                .getOrDefault(builder.getId(), Collections.emptySet())
                                .contains(file.getPath()))
                            .collect(Collectors.toList()));
                    builder.getStorageDataBuilder().clearFile();
                });
        }

        // populate connected networks for VMs and then clear their layeredOver relations
        vmDtoById.values().forEach(vm -> {
            vm.getVirtualMachineDataBuilder().addAllConnectedNetwork(
                    vm.getLayeredOverList().stream()
                        .map(networkIds::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
            );
            vm.clearLayeredOver();
        });
    }

    /**
     * Convert the EntityDTOs so that the provider IDs match the entity IDs assigned to the
     * providers in the id field of the EntityDTO. TopologyProcessor will reject any DTOs if it
     * doesn't recognize the providerIds within the same discoveryResponse.
     *
     * The EntityDTO after conversion is identical to the original one except the providers all
     * have the "dsb-" prefix attached to their IDs. This is needed because the classic storage
     * browsing probe adds a dsb- prefix to the ids of Storage and Virtual Machines it discovers to
     * distinguish them from the same entity discovered by the VC probe. However, when referring to
     * those same Storages as providers for the VMs, it does not add the prefix.  Classic seems to
     * handle this gracefully, but XL expects all provider ids that a probe discovers to appear
     * somewhere in that probes discovery response as discovered entities.
     *
     * @param entityBuilder the entity builder for which the CommodityBought id should be converted
     */
    private void convertCommodityBoughtProviderId(@Nonnull EntityDTO.Builder entityBuilder) {
        entityBuilder.getCommoditiesBoughtBuilderList().forEach(cbBuilder ->
            cbBuilder.setProviderId(STORAGE_BROWSING_ID_PREFIX + cbBuilder.getProviderId()));
    }

    /**
     * Create one Volume for each VirtualMachine/Storage pair where the Storage hosts files that are
     * used by that VirtualMachine.
     *
     * @return List of EntityDTOs representing the VirtualVolumes that were created.
     */
    private List<EntityDTO> createVirtualVolumesFromVms() {
        return vmToStorageFiles.rowKeySet().stream()
                .flatMap(vmId -> {
                    final EntityDTO.Builder vmDTO = vmDtoById.get(vmId);
                    return vmToStorageFiles.row(vmId).keySet().stream()
                        .map(storageId -> {
                            final EntityDTO volume = createVirtualVolume(vmId, storageId);
                            // set up layeredOver from vm to volume
                            vmDTO.addLayeredOver(volume.getId());
                            // if it's not storage browsing, also try to associate the commodity
                            // bought with this volume
                            if (!isStorageBrowsing) {
                                final CommodityBought.Builder commodityBought =
                                        vmToStorageCommodityBought.get(vmId, storageId);
                                if (commodityBought != null) {
                                    commodityBought.setSubDivision(SubDivisionData.newBuilder()
                                            .setSubDivisionId(volume.getId()));
                                } else {
                                    logger.error("VM {} is not buying commodity from storage {}",
                                            vmDTO.getId(), storageId);
                                }
                            }
                            return volume;
                        });
                }).collect(Collectors.toList());
    }

    private EntityDTO createVirtualVolume(@Nonnull String vmId, @Nonnull String storageId) {
        Objects.requireNonNull(vmId);
        Objects.requireNonNull(storageId);

        final EntityDTO.Builder storage = storageDtoById.get(storageId);
        if (storage == null) {
            logger.warn("No storage found with ID {} for VM with ID {}", storageId, vmId);
        }

        EntityDTO.Builder entityBuilder = EntityDTO.newBuilder()
            .setId(combineStringsForVolumeNaming(addPrefix(vmId), addPrefix(storageId)))
            .setDisplayName(combineStringsForVolumeNaming(vmDtoById.get(vmId).getDisplayName(),
                //todo: when will this be null? ask Ron?
                (storage != null && storage.hasDisplayName() ?
                    storage.getDisplayName() : storageId)))
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .addLayeredOver(storageId)
            .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                .addAllFile(getVmFileList(vmId, storageId)));
        // Storage browsing data may be out of date.  Discard volumes that don't get stitched with
        // a volume from the main probe.
        if (isStorageBrowsing) {
            entityBuilder.setOrigin(EntityOrigin.PROXY);
            entityBuilder.setKeepStandalone(false);
        }
        return entityBuilder.build();
    }

    /**
     * Created a "Wasted Files" virtual volume for each Storage that has files associated with it.
     *
     * @return List of EntityDTOs of the virtual volumes that were created.
     */
    private List<EntityDTO> createVirtualVolumesFromStorages() {
        return storageDtoById.keySet().stream()
                .map(this::createWastedFilesVirtualVolume)
                .collect(Collectors.toList());
    }

    private EntityDTO createWastedFilesVirtualVolume(@Nonnull String storageId) {
        Objects.requireNonNull(storageId);
        return EntityDTO.newBuilder()
                .setId(combineStringsForVolumeNaming(storageId, WASTED_VOLUMES_SUFFIX))
                .setDisplayName(combineStringsForVolumeNaming(
                    storageDtoById.get(storageId).getDisplayName(), WASTED_VOLUMES_SUFFIX))
                .setEntityType(EntityType.VIRTUAL_VOLUME)
                .addLayeredOver(storageId)
                .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                        .addAllFile(getStorageFileList(storageId)))
                .build();
    }

    private List<VirtualVolumeFileDescriptor> getVmFileList(String vmId, String storageId) {
        return vmToStorageFiles.get(vmId, storageId).stream()
                .map(vmFileDescr ->
                    VirtualVolumeFileDescriptor.newBuilder()
                            .setPath(vmFileDescr.getPath())
                            .setSizeKb(vmFileDescr.getSizeKb())
                            .setType(vmFileDescr.getType())
                    .build())
                .collect(Collectors.toList());
    }

    /**
     * Return all the files on this storage.
     *
     * @param storageId of the Storage.
     * @return List of {@link VirtualVolumeFileDescriptor}s representing files on the Storage.
     */
    private List<VirtualVolumeFileDescriptor> getStorageFileList(String storageId) {
        // Set of file paths on this Storage used by VMs
        return storageIdToFiles.getOrDefault(storageId, Collections.emptyList()).stream()
            .map(storageFileDescriptor ->
                VirtualVolumeFileDescriptor.newBuilder()
                    .setPath(storageFileDescriptor.getPath())
                    .setSizeKb(storageFileDescriptor.getSizeKb())
                    .setType(storageFileDescriptor.getType())
                    .setModificationTimeMs(storageFileDescriptor.getModificationTimeMs())
                    .addAllLinkedPaths(storageFileDescriptor.getOtherPathsList())
                    .build())
            .collect(Collectors.toList());
    }

    /**
     * If it's storage browsing, we don't need to do anything since it's already prefixed with
     * {@link AddVirtualVolumeDiscoveryConverter#STORAGE_BROWSING_ID_PREFIX}. If not, we should add
     * the prefix to its entity id, since we should ensure id is consistent between volume created
     * from vc probe and vc storage browsing probe. This is needed to ensure stitching happens
     * correctly.
     *
     * @param entityId the entity id to add prefix to
     * @return entity id prefixed with {@link AddVirtualVolumeDiscoveryConverter#STORAGE_BROWSING_ID_PREFIX}
     */
    private String addPrefix(String entityId) {
        return this.isStorageBrowsing ? entityId : STORAGE_BROWSING_ID_PREFIX + entityId;
    }

    /**
     * Utility method for creating VirtualVolume ID and displayname.
     *
     * @param firstPart the first part of the id or display name.
     * @param secondPart the second part of the id or display name.
     * @return a combined string beginning with VIRTUAL_VOLUME_ID_PREFIX and separating the two
     * arguments by ID_SEPARATOR
     */
    @VisibleForTesting
    public static String combineStringsForVolumeNaming(String firstPart, String secondPart) {
        return VIRTUAL_VOLUME_ID_PREFIX + firstPart + ID_SEPARATOR + secondPart;
    }
}
