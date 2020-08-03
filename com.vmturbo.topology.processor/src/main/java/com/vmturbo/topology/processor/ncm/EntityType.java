package com.vmturbo.topology.processor.ncm;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.AVAILABILITY_ZONE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CHASSIS;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER_POD;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER_POD_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATACENTER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.REGION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_DATACENTER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;

/**
 * The EntityType is utility class providing convenience methods for various entity types.
 */
public class EntityType {
    /**
     * No instantiation available.
     */
    private EntityType() {
    }

    /**
     * Checks whether entity is a container pod.
     *
     * @param e The entity.
     * @return {@code true} iff e is a container pod.
     */
    public static boolean isContainerPod(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(CONTAINER_POD);
    }

    /**
     * Checks whether entity is a container pod.
     *
     * @param e The entity.
     * @return {@code true} iff e is a container pod.
     */
    public static boolean isContainerPod(final @Nonnull TopologyEntity e) {
        return e.getEntityType() == CONTAINER_POD_VALUE;
    }

    /**
     * Checks whether entity is a VM.
     *
     * @param e The entity.
     * @return {@code true} iff e is a VM.
     */
    public static boolean isVM(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(VIRTUAL_MACHINE);
    }

    /**
     * Checks whether entity is a PM.
     *
     * @param e The entity.
     * @return {@code true} iff e is a PM.
     */
    public static boolean isPM(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(PHYSICAL_MACHINE);
    }

    /**
     * Checks whether entity is a Chassis.
     *
     * @param e The entity.
     * @return {@code true} iff e is a Chassis.
     */
    public static boolean isChassis(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(CHASSIS);
    }

    /**
     * Checks whether entity is a AZ.
     *
     * @param e The entity.
     * @return {@code true} iff e is a AZ.
     */
    public static boolean isAZ(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(AVAILABILITY_ZONE);
    }

    /**
     * Checks whether entity is a Datacenter.
     *
     * @param e The entity.
     * @return {@code true} iff e is a Datacenter.
     */
    public static boolean isDC(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(DATACENTER);
    }

    /**
     * Checks whether entity is a Virtual Datacenter.
     *
     * @param e The entity.
     * @return {@code true} iff e is a Virtual Datacenter.
     */
    public static boolean isVDC(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(VIRTUAL_DATACENTER);
    }

    /**
     * Checks whether entity is a Region.
     *
     * @param e The entity.
     * @return {@code true} iff e is a Region.
     */
    public static boolean isRegion(final @Nonnull StitchingEntity e) {
        return e.getEntityType().equals(REGION);
    }

}
