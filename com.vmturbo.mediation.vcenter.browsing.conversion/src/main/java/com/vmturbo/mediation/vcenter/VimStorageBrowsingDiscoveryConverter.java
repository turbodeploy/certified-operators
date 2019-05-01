package com.vmturbo.mediation.vcenter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StorageFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VirtualMachineFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * A utility class that converts the {@link DiscoveryResponse} from a
 * {@link com.vmturbo.mediation.vmware.browsing.sdk.VimStorageBrowsingProbe} to the format needed
 * for the {@link VimStorageBrowsingConversionProbe}.  We keep the file information that is
 * originally in the virtual_machine_data in a VirtualVolume that is connected to the VM and the
 * Storage.  We also create a VirtualVolume associated with each Storage to hold all the files
 * in the storage_data.  This VirtualVolume will eventually only hold the wasted files after a
 * poststitching operation removes any files associated with a VM.
 */
public class VimStorageBrowsingDiscoveryConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The discovery response from the VimStorageBrowsingProbe.
     */
    private final DiscoveryResponse originalDiscoveryResponse;

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
     * A map from VM id to its EntityDTO.
     */
    private final Map<String, EntityDTO.Builder> vmDtoById = Maps.newHashMap();

    /**
     * A map from storage id to its EntityDTO.
     */
    private final Map<String, EntityDTO> storageDtoById = Maps.newHashMap();

    /**
     * A prefix we attach to the beginning of the VirtualVolume ID.
     */
    private static final String VIRTUAL_VOLUME_ID_PREFIX = "VirtualVolume-";

    /**
     * A suffix we add to the VirtualVolume ID for those virtual volumes representing wasted files.
     */
    @VisibleForTesting
    static final String WASTED_VOLUMES_SUFFIX = "wastedFiles";

    private static final String ID_SEPARATOR = "-";

    /**
     * The prefix that the VimStorageBrowsing probe attaches to VM and Storage IDs in the ID field
     * of the EntityDTO but NOT in the provider id field or in the storageId field of the
     * VirtualMachineFileDescriptor.
     */
    @VisibleForTesting
    static final String STORAGE_BROWSING_ID_PREFIX = "dsb-";

    public VimStorageBrowsingDiscoveryConverter(@Nonnull DiscoveryResponse discoveryResponse) {
        originalDiscoveryResponse = Objects.requireNonNull(discoveryResponse);
        discoveryResponseBuilder = DiscoveryResponse.newBuilder(discoveryResponse);
        convertIds(discoveryResponseBuilder);
    }

    /**
     * Convert the EntityDTOs so that the provider IDs match the entity IDs assigned to the providers
     * in the id field of the EntityDTO.  TopologyProcessor will reject any DTOs if it doesn't
     * recognize the providerIds within the same discoveryResponse.
     *
     * @param discoveryResponseBuilder the builder for creating the new discovery response.
     */
    private void convertIds(@Nonnull DiscoveryResponse.Builder discoveryResponseBuilder) {
        List<EntityDTO> entities = discoveryResponseBuilder.getEntityDTOList().stream()
                .map(this::convertProviderIds)
                .collect(Collectors.toList());
        discoveryResponseBuilder.clearEntityDTO().addAllEntityDTO(entities);
    }

    /**
     * return an EntityDTO that is identical to the original one except the providers all have the
     * dsb- prefix attached to their IDs.  This is needed because the classic storage browsing
     * probe adds a dsb- prefix to the ids of Storage and Virtual Machines it discovers to
     * distinguish them from the same entity discovered by the VC probe.  However, when referring to
     * those same Storages as providers for the VMs, it does not add the prefix.  Classic seems to
     * handle this gracefully, but XL expects all provider ids that a probe discovers to appear
     * somewhere in that probes discovery response as discovered entities.
     *
     * @param entity the EntityDTO to convert.
     * @return EntityDTO where all providers have their ids prefixed by dsb-.
     */
    private EntityDTO convertProviderIds(EntityDTO entity) {
        EntityDTO.Builder retVal = entity.toBuilder();
        retVal.getCommoditiesBoughtBuilderList().stream()
                .forEach(builder -> builder.setProviderId(STORAGE_BROWSING_ID_PREFIX +
                        builder.getProviderId()));
        return retVal.build();
    }

    /**
     * Convert old discovery response to new discovery response consistent with the XL model.
     *
     * @return new discovery response
     */
    public DiscoveryResponse convert() {
        // pre process discovery response to create data structures with relationships among
        // files, virtual machines and storages.  These will be the basis for virtual volume
        // creation.
        preProcess();

        // create volumes between VMs and Storages and volumes just connected with storages
        List<EntityDTO> volumes = createVirtualVolumesFromVms();
        volumes.addAll(createVirtualVolumesFromStorages());

        // clear the entityDTOs and just add back the ones that have files and the virtual volumes.
        // Also, VM DTOs will now have layeredOver relationship with Volume which topology-processor
        // will convert to connectedTo/connectedFrom and VirtualVolumes will have layeredOver
        // relationships with Storages.
        discoveryResponseBuilder.clearEntityDTO();
        discoveryResponseBuilder.addAllEntityDTO(storageDtoById.values());
        discoveryResponseBuilder.addAllEntityDTO(vmDtoById.values().stream()
                .map(EntityDTO.Builder::build)
                .collect(Collectors.toList()));
        discoveryResponseBuilder.addAllEntityDTO(volumes);
        return discoveryResponseBuilder.build();
    }

    private void preProcess() {
        // iterate over DTOs and populate the Table of VMFileDescriptors indexed by VM id and
        // storage id.
        discoveryResponseBuilder.getEntityDTOList().stream()
                .filter(EntityDTO::hasVirtualMachineData)
                .forEach(entityDTO -> {
                    vmDtoById.put(entityDTO.getId(), entityDTO.toBuilder());
                    entityDTO.getVirtualMachineData().getFileList().stream()
                            .filter(VirtualMachineFileDescriptor::hasStorageId)
                            .forEach(fileDescriptor -> {
                                vmToStorageFiles.row(entityDTO.getId())
                                        .computeIfAbsent(STORAGE_BROWSING_ID_PREFIX
                                                + fileDescriptor.getStorageId(),
                                                s -> Lists.newArrayList())
                                        .add(fileDescriptor);
                            });
                });

        // populate the map of storage ID to the files that exist on that storage
        discoveryResponseBuilder.getEntityDTOList().stream()
                .filter(EntityDTO::hasStorageData)
                .forEach(entityDTO -> {
                    storageDtoById.put(entityDTO.getId(), entityDTO);
                    storageIdToFiles.put(entityDTO.getId(),
                            entityDTO.getStorageData().getFileList());
                });
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
                    List<EntityDTO> newVirtualVolumesForVm = vmToStorageFiles.row(vmId).keySet()
                            .stream()
                            .map(storageId -> createVirtualVolume(vmId, storageId))
                            .collect(Collectors.toList());

                    EntityDTO.Builder vmDto = vmDtoById.remove(vmId);
                    vmDtoById.put(vmId, vmDto
                            .addAllLayeredOver(newVirtualVolumesForVm.stream()
                                    .map(EntityDTO::getId)
                                    .collect(Collectors.toList())));
                    return newVirtualVolumesForVm.stream();
                }).collect(Collectors.toList());
    }

    /**
     * Created a "Wasted Files" virtual volume for each Storage that has files associated with it.
     *
     * @return List of EntityDTOs of the virtual volumes that were created.
     */
    private List<EntityDTO> createVirtualVolumesFromStorages() {
        return storageIdToFiles.keySet().stream()
                .map(storageId -> createWastedFilesVirtualVolume(storageId))
                .collect(Collectors.toList());
    }

    private EntityDTO createVirtualVolume(@Nonnull String vmId, @Nonnull String storageId) {
        Objects.requireNonNull(vmId);
        Objects.requireNonNull(storageId);
        EntityDTO storage = storageDtoById.get(storageId);
        if (storage == null) {
            logger.warn("No storage found with ID {} for VM with ID {}", storageId, vmId);
        }
        return EntityDTO.newBuilder()
                .setId(combineStringsForVolumeNaming(vmId, storageId))
                .setDisplayName(combineStringsForVolumeNaming(vmDtoById.get(vmId).getDisplayName(),
                        (storage != null && storage.hasDisplayName() ?
                                storage.getDisplayName() : storageId)))
                .setEntityType(EntityType.VIRTUAL_VOLUME)
                .addLayeredOver(storageId)
                .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                        .addAllFile(getVmFileList(vmId, storageId)))
                .build();
    }

    private EntityDTO createWastedFilesVirtualVolume(@Nonnull String storageId) {
        Objects.requireNonNull(storageId);
        return EntityDTO.newBuilder()
                .setId(combineStringsForVolumeNaming(storageId, WASTED_VOLUMES_SUFFIX))
                .setDisplayName(combineStringsForVolumeNaming(
                        storageDtoById.get(storageId).getDisplayName(),
                        WASTED_VOLUMES_SUFFIX))
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
        return storageIdToFiles.get(storageId).stream()
                .map(storageFileDescriptor ->
                        VirtualVolumeFileDescriptor.newBuilder()
                                .setPath(storageFileDescriptor.getPath())
                                .setSizeKb(storageFileDescriptor.getSizeKb())
                                .setType(storageFileDescriptor.getType())
                                .setModificationTimeMs(storageFileDescriptor
                                        .getModificationTimeMs())
                                .addAllLinkedPaths(storageFileDescriptor
                                         .getOtherPathsList())
                                .build())
                .collect(Collectors.toList());
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
    static String combineStringsForVolumeNaming(String firstPart, String secondPart) {
        return VIRTUAL_VOLUME_ID_PREFIX + firstPart + ID_SEPARATOR
                + secondPart;
    }
}
