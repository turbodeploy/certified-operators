package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

/**
 * Additional properties that the system supports for searching.
 *
 * <p/>Separated out from the main {@link TopologyGraphEntity} to avoid too many methods,
 * and to make it easier to implement graphs that do not support searching.
 */
public interface SearchableProps {
    /**
     * These are the commodity types that need to be persisted and returned by
     * because we support searching on their
     * values.
     *
     * <p/>Implementations may or may not persist additional commodities.
     */
    Set<Integer> SEARCHABLE_COMM_TYPES =
            ImmutableSet.of(CommodityType.VMEM_VALUE, CommodityType.MEM_VALUE,
                CommodityType.ACTIVE_SESSIONS_VALUE, CommodityType.TOTAL_SESSIONS_VALUE,
                            CommodityType.CLUSTER_VALUE);

    /**
     * Get the {@link TagIndex} for the topology graph. We do not expose tags on a per-entity basis
     * directly (except through the tag index).
     *
     * @return The {@link TagIndex}, used to resolve tag searches.
     */
    @Nonnull
    TagIndex getTagIndex();

    /**
     * Checks whether entity has bought commodity of the specified {@link CommodityType}. Returns
     * {@code true} in case it has such a commodity, otherwise it returns {@code false}.
     *
     * @param commodityType type of the commodity which existence has to be
     *                 verified.
     * @param providerType type of the provider that from which commodity has to be bought.
     * @return {@code true} in case entity has a commodity with that type, otherwise returns
     *                 {@code false}.
     */
    boolean hasBoughtCommodity(@Nonnull CommodityType commodityType,
                    @Nullable EntityType providerType);

    /**
     * Get the used value of a particular commodity type. We do not return a "commodity" object to
     * reduce the amount of objects we need to create for "thinner" implementations of searchable
     * properties.
     *
     * @param type The commodity type.
     * @return The used value or -1 if there is no commodity of this type.
     */
    float getCommodityUsed(int type);

    /**
     * Get the capacity of a particular commodity type. We do not return a "commodity" object to
     * reduce the amount of objects we need to create for "thinner" implementations of searchable
     * properties.
     *
     * @param type The commodity type.
     * @return The capacity. or -1 if there is no commodity of this type.
     */
    float getCommodityCapacity(int type);

    /**
     * Checks whether the VM supports hot add feature for the selected type of commodity.
     *
     * @param commodityType The commodity type.
     * @return Returns true if the feature is supported, otherwise false.
     */
    boolean isHotAddSupported(int commodityType);

    /**
     * Checks whether the VM supports hot remove feature for the selected type of commodity.
     *
     * @param commodityType The commodity type.
     * @return Returns true if the feature is supported, otherwise false.
     */
    boolean isHotRemoveSupported(int commodityType);

    /**
     * Searchable properties specific to host entities.
     */
    interface PmProps extends SearchableProps {
        /**
         * Get the number of CPUs on the host.
         *
         * @return The number of CPUs.
         */
        int getNumCpus();

        /**
         * The vendor (or manufacturer) of the host.
         *
         * @return The vendor name.
         */
        @Nonnull
        String getVendor();

        /**
         * Get the CPU model for this machine.
         *
         * @return The CPU model.
         */
        @Nonnull
        String getCpuModel();

        /**
         * Get the model identifier.
         *
         * @return The model identifier.
         */
        @Nonnull
        String getModel();

        /**
         * Get the timezone of the host.
         *
         * @return The timezone.
         */
        @Nonnull
        String getTimezone();
    }

    /**
     * Searchable properties specific to storage entities.
     */
    interface StorageProps extends SearchableProps {
        /**
         * Return whether or not the storage is local to a specific host (as opposed to a shared
         * storage).
         *
         * @return True if it is local.
         */
        boolean isLocal();
    }

    /**
     * Searchable properties specific to virtual machines.
     */
    interface VmProps extends SearchableProps {
        /**
         * Get the names of connected networks. We do not currently have real connections between
         * VMs and networks, so we use names.
         *
         * @return The list of connected network names.
         */
        @Nonnull
        Collection<String> getConnectedNetworkNames();

        /**
         * The name of the guest OS.
         *
         * @return The guest OS name.
         */
        @Nonnull
        String getGuestOsName();

        /**
         * Get the number of VCPUs on the eVM.
         *
         * @return The number of VCPUs.
         */
        int getNumCpus();

        /**
         * Get the number of VM sockets.
         *
         * @return the number of sockets.
         */
        default int getNumberOfSockets() {
            final int rawCps = getCoresPerSocket();
            final double cps = rawCps <= 0 ? 1 : rawCps;
            return (int)Math.ceil(getNumCpus() / cps);
        }

        /**
         * Get the cores per socket ratio for the VM.
         *
         * @return the cores per socket ratio for the VM.
         */
        int getCoresPerSocket();

        /**
         * Get the version of tools if available for the VM.
         * Method will return null to indicate tools is not installed.
         * For filtering such VMs, resetting empty string of tool version to null is expected by StringPredicate
         * matching logic in TopologyFilterFactory.
         *
         * @return The version of vendor tool.
         */
        String getVendorToolsVersion();

        /**
         * Get the value for isVDI (Used to identify whether the VM is a part of a VDI instance or not.
         *
         * @return Returns true if the VM is a part of VDI instance, otherwise false.
         */
        boolean isVdi();

    }

    /**
     * Searchable properties for volumes.
     */
    interface VolumeProps extends SearchableProps {
        /**
         * Attachment state of the volume.
         *
         * @return The attachment state.
         */
        @Nonnull
        AttachmentState attachmentState();

        /**
         * Whether or not the volume is encrypted.
         *
         * @return True if it is encrypted.
         */
        boolean isEncrypted();

        /**
         * Whether or not the volume is ephemeral.
         * @return True if it is ephemeral.
         */
        boolean isEphemeral();

        /**
         * Whether or not the volume is deletable.
         * @return True if it is deletable.
         */
        boolean isDeletable();

        /**
         * Number of days unattached.
         */
        @Nonnull
        Optional<Integer> daysUnattached();
    }

    /**
     * Searchable properties for services.
     */
    interface ServiceProps extends SearchableProps {
        /**
         * Get the kubernetes service type.
         *
         * @return the kubernetes service type
         */
        @Nullable
        String getKubernetesServiceType();
    }

    /**
     * Searchable properties for workload controllers.
     */
    interface WorkloadControllerProps extends SearchableProps {
        /**
         * Get the controller type.
         *
         * @return The controller type.
         */
        @Nonnull
        String getControllerType();

        /**
         * Whether this is a custom controller type.
         *
         * @return True if it is a custom controller type.
         */
        boolean isCustom();
    }

    /**
     * Searchable properties for business accounts.
     */
    interface BusinessAccountProps extends SearchableProps {

        /**
         * Whether or not the account has an associated target.
         *
         * @return True if there is a target associated with this account.
         */
        boolean hasAssociatedTargetId();

        /**
         * The account ID used by the service provider (e.g. subscription ID in Azure).
         *
         * @return The account ID.
         */
        @Nonnull
        String getAccountId();
    }

    /**
     * Searchable properties for database server.
     */
    interface DatabaseServerProps extends SearchableProps {

        /**
         * Database engine.
         *
         * @return engine name.
         */
        @Nonnull
        DatabaseEngine getDatabaseEngine();

        /**
         * Database edition.
         *
         * @return DB edition.
         */
        @Nonnull
        String getDatabaseEdition();

        /**
         * Database version.
         *
         * @return version.
         */
        @Nonnull
        String getDatabaseVersion();

        /**
         * Get Storage Encryption state.
         *
         * @return Storage Encryption state
         */
        String getStorageEncryption();

        /**
         * Get StorageAutoscaling state.
         *
         * @return StorageAutoscaling state
         */
        String getStorageAutoscaling();

        /**
         * Get PerformanceInsights state.
         *
         * @return PerformanceInsights state
         */
        String getPerformanceInsights();

        /**
         * Get Cluster Role.
         *
         * @return Cluster Role
         */
        String getClusterRole();

        /**
         * Get Storage Tier.
         *
         * @return Storage Tier
         */
        String getStorageTier();
    }

    /**
     * Searchable properties for database.
     */
    interface DatabaseProps extends SearchableProps {
        /**
         * Get Replication Role.
         *
         * @return Replication Role
         */

        String getReplicationRole();
        /**
         * Get Pricing Model.
         *
         * @return Pricing Model
         */

        String getPricingModel();
        /**
         * Get Service Tier.
         *
         * @return Service Tier
         */

        String getServiceTier();
    }

    /**
     * Searchable properties for virtual machine spec.
     */
    interface VirtualMachineSpecProps extends SearchableProps {
        /**
         * Get Tier.
         *
         * @return Tier
         */
        String getTier();

        /**
         * Get App Count.
         *
         * @return App Count
         */

        Integer getAppCount();

        /**
         * Get the number of days empty, meaning number of days hosting 0 apps.
         *
         * @return The number of days empty
         */
        Optional<Integer> getDaysEmpty();
    }

    /**
     * Searchable properties for the ComputeTier entity type.
     */
    interface ComputeTierProps extends SearchableProps {

        /**
         * VirtualMachine profile portion in the vendor ID of the ComputeTier.
         */
        String VMPROFILE = "VMPROFILE";

        /**
         * VirtualMachineSpec profile portion in the vendor ID of the ComputeTier.
         */
        String VMSPECPROFILE = "VMSPECPROFILE";

        /**
         * Get the consumer entity types of a ComputeTier.
         *
         * @return a list of {@link EntityType}s.
         */
        Set<EntityType> getConsumerEntityTypes();

        /**
         * Get the consumer {@link EntityType} associated with the given vendorId. Given
         * azure::VMPROFILE::Standard_D4s_v3, the VirtualMachine EntityType would be
         * returned, given azure::VMSPECPROFILE::F1 the VirtualMachineSpec EntityTYpe would
         * be returned.
         *
         * @param vendorId The vendor ID.
         * @return The entity type associated with the vendor ID.
         */
        static EntityType getConsumerEntityType(String vendorId) {
            if (vendorId == null || vendorId.trim().isEmpty()) {
                return null;
            }
            // Vendor ID for azure compute tiers the vendorId is:
            // azure::VMPROFILE::Standard_D4s_v3
            // but for aws and gcp the compute tier the vendorId is changed and only include display
            // name, so we will assume by default the consumer entity type of a compute tier is a VM
            final String[] vendorIdParts = vendorId.split("::");
            if (vendorIdParts.length != 3) {
                return EntityType.VIRTUAL_MACHINE;
            }
            switch (vendorIdParts[1]) {
                case VMSPECPROFILE:
                    return EntityType.VIRTUAL_MACHINE_SPEC;
                case VMPROFILE:
                default:
                    return EntityType.VIRTUAL_MACHINE;
            }
        }
    }
}
