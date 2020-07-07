package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;

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
                CommodityType.ACTIVE_SESSIONS_VALUE, CommodityType.TOTAL_SESSIONS_VALUE);

    /**
     * Get the {@link TagIndex} for the topology graph. We do not expose tags on a per-entity basis
     * directly (except through the tag index).
     *
     * @return The {@link TagIndex}, used to resolve tag searches.
     */
    @Nonnull
    TagIndex getTagIndex();

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

}
