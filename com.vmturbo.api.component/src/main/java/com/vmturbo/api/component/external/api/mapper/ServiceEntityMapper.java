package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ServiceEntityMapper {

    /**
     * The entity types used in UI.
     */
    enum UIEntityType {
        VIRTUAL_MACHINE("VirtualMachine"),
        PHYSICAL_MACHINE("PhysicalMachine"),
        STORAGE("Storage"),
        DATACENTER("DataCenter"),
        DISKARRAY("DiskArray"),
        VIRTUAL_DATACENTER("VirtualDataCenter"),
        APPLICATION("Application"),
        VIRTUAL_APPLICATION("VirtualApplication"),
        CONTAINER("Container"),
        VPOD("VPod"),
        DPOD("DPod"),
        STORAGECONTROLLER("StorageController"),
        IOMODULE("IOModule"),
        INTERNET("Internet"),
        SWITCH("Switch"),
        CHASSIS("Chassis"),
        NETWORK("Network"),
        UNKNOWN("Unknown");

        private final String value;

        UIEntityType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Converts type from a string to the enum type.
         * @param type string representation of service entity type
         * @return UI entity type enum
         */
        public static UIEntityType fromString(String type) {
            if (type != null) {
                for (UIEntityType t : UIEntityType.values()) {
                    if (type.equals(t.value)) {
                        return t;
                    }
                }
            }

            throw new IllegalArgumentException(
                       "No UIEntityType constant with type " + type + " found");
        }
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    static final BiMap<Integer, UIEntityType> ENTITY_TYPE_MAPPINGS =
               new ImmutableBiMap.Builder<Integer, UIEntityType>()
            .put(EntityType.VIRTUAL_MACHINE.getNumber(),        UIEntityType.VIRTUAL_MACHINE)
            .put(EntityType.PHYSICAL_MACHINE.getNumber(),       UIEntityType.PHYSICAL_MACHINE)
            .put(EntityType.STORAGE.getNumber(),                UIEntityType.STORAGE)
            .put(EntityType.DISK_ARRAY.getNumber(),             UIEntityType.DISKARRAY)
            .put(EntityType.DATACENTER.getNumber(),             UIEntityType.DATACENTER)
            .put(EntityType.VIRTUAL_DATACENTER.getNumber(),     UIEntityType.VIRTUAL_DATACENTER)
            .put(EntityType.APPLICATION.getNumber(),            UIEntityType.APPLICATION)
            .put(EntityType.VIRTUAL_APPLICATION.getNumber(),    UIEntityType.VIRTUAL_APPLICATION)
            .put(EntityType.CONTAINER.getNumber(),              UIEntityType.CONTAINER)
            .put(EntityType.STORAGE_CONTROLLER.getNumber(),     UIEntityType.STORAGECONTROLLER)
            .put(EntityType.IO_MODULE.getNumber(),              UIEntityType.IOMODULE)
            .put(EntityType.INTERNET.getNumber(),               UIEntityType.INTERNET)
            .put(EntityType.SWITCH.getNumber(),                 UIEntityType.SWITCH)
            .put(EntityType.CHASSIS.getNumber(),                UIEntityType.CHASSIS)
            .put(EntityType.NETWORK.getNumber(),                UIEntityType.NETWORK)
            .put(EntityType.UNKNOWN.getNumber(),                UIEntityType.UNKNOWN)
            .build();

    /**
     * Maps the entity type in TopologyEntityDTO to strings of entity types used in UI.
     *
     * @param type The entity type in the TopologyEntityDTO
     * @return     The corresponding entity type string in UI
     */
    public static String toUIEntityType(@Nonnull final int type) {
        final UIEntityType uiEntityType = ENTITY_TYPE_MAPPINGS.get(type);

        if (uiEntityType == null) {
            return EntityType.forNumber(type).toString();
        }

        return uiEntityType.getValue();
    }

    /**
     * Maps the entity type used in UI to the type used in TopologyEntityDTO.
     *
     * @param uiEntityType The entity type string used in UI
     * @return The type used in TopologyEntityDTO
     */
    public static int fromUIEntityType(@Nonnull final String uiEntityType) {
        final int topologyEntityType = ENTITY_TYPE_MAPPINGS.inverse()
                        .getOrDefault(UIEntityType.fromString(uiEntityType),
                                      EntityType.UNKNOWN.getNumber());
        return topologyEntityType;
    }

    /**
     * Convert numerical representation of the state used in TopologyEntityDTO
     * to a string value that the UI understands.
     * @param state numerical representation of state
     * @return string representation of state
     *
     * TODO: Refactor this and RepoObjectState so this conversion lives in one place.
     */
    static String toState(int state) {
        switch (state) {
            case EntityState.POWERED_ON_VALUE: return "ACTIVE";
            case EntityState.POWERED_OFF_VALUE: return "IDLE";
            case EntityState.SUSPENDED_VALUE: return "SUSPENDED";
            case EntityState.MAINTENANCE_VALUE: return "MAINTENANCE";
            case EntityState.FAILOVER_VALUE: return "FAILOVER";
            default: return "UNKNOWN";
        }
    }
}
