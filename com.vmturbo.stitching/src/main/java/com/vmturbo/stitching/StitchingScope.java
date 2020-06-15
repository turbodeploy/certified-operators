package com.vmturbo.stitching;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A {@link StitchingScope} determines which entities will be fed to the
 * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)} method.
 *
 * @param <ENTITY> The class of entity that stitching will operate on. Examples are {@link StitchingEntity}
 *                 or {@link TopologyEntity}.
 */
public interface StitchingScope<ENTITY> {
    /**
     * Get the entities in the stitching scope.
     *
     * @return A stream of the entities in the stitching scope.
     */
    @Nonnull
    Stream<ENTITY> entities();

    /**
     * A factory for construction {@link StitchingScope}s.
     *
     * Use this factory in {@link PreStitchingOperation#getScope(StitchingScopeFactory)} to
     * construct the appropriate scope for the calculation.
     */
    interface StitchingScopeFactory<ENTITY> {
        /**
         * Return a global calculation scope. Calculations with this scope will operate on all entities
         * in the entire topology.
         *
         * @return A global {@link StitchingScope}.
         */
        StitchingScope<ENTITY> globalScope();

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only
         * entities discovered by a specific probe type.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @return A probe type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> probeScope(@Nonnull final String probeTypeName);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given entity type. Calculations with this scope will be fed all entities of a given
         * {@link EntityType} regardless of which target or probe discovered those entities.
         *
         * @param entityType The entity type of the entities
         * @return An entity type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> entityTypeScope(@Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of the given entity types. Calculations with this scope will be fed all entities which match
         * the list of the the given {@link EntityType}s regardless of which target or probe discovered
         * those entities.
         *
         * @param entityTypes List of entity type of the entities
         * @return An entity type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> multiEntityTypesScope(@Nonnull final List<EntityType> entityTypes);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of the given entity types. Calculations with this scope will be fed all entities which match
         * the list of the the given {@link EntityType}s regardless of which target or probe discovered
         * those entities. Entities must exist for all provided {@link EntityType}s, if no entities
         * for any {@link EntityType}, it will return empty scope.
         *
         * @param entityTypes List of entity type of the entities
         * @return An entity type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> containsAllEntityTypesScope(@Nonnull final List<EntityType> entityTypes);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         type of probe.
         */
        StitchingScope<ENTITY> probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                    @Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a set of specific type of probes.
         *
         * @param probeTypeNames Set of names of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         type of probe.
         */
        StitchingScope<ENTITY> multiProbeEntityTypeScope(@Nonnull final Set<String> probeTypeNames,
                                                    @Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe category.
         *
         * @param probeCategory The category of probe whose entities should be fed to the calculation.
         *                      Example: HYPERVISOR or STORAGE
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         category of probe.
         */
        StitchingScope<ENTITY> probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                            @Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * discovered by a specific probe category.
         *
         * @param probeCategory The category of probe whose entities should be fed to the calculation.
         *                      Example: HYPERVISOR or STORAGE
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         category of probe.
         */
        StitchingScope<ENTITY> probeCategoryScope(@Nonnull ProbeCategory probeCategory);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe category.
         *
         * @param probeCategories The set of categories of probes whose entities should be fed to
         *                        the calculation.
         *                        Example: {HYPERVISOR, STORAGE}
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         set of categories of probes.
         */
        StitchingScope<ENTITY> multiProbeCategoryEntityTypeScope(
                @Nonnull final Set<ProbeCategory> probeCategories,
                @Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe and not discovered
         * by a derived target of another type of probe.  This is used for identifying shared
         * storages that are we don't have complete storage browsing information for.
         *
         * @param parentProbeType The name of the probe type of the main probe that discovers the
         *                        entity.
         * @param childProbeType The name of the probe type that is the derived target of the main
         *                       probe type.
         * @param entityType The type of entity to find.
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered
         * by multiple targets of the parentProbeType but missing at least one matching target of
         * childProbeType.
         */
        StitchingScope<ENTITY> missingDerivedTargetEntityTypeScope(
            @Nonnull String parentProbeType,
            @Nonnull String childProbeType,
            @Nonnull EntityType entityType);


        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by some probe categories but NOT discovered by other probe categories.
         * This is used for identifying
         * VMs that has a hypervisor target but has no guestosprocesses target, we will disable vmem resizing for such VMs.
         *
         * @param owningProbeCategories The probe categories that VMs have
         * @param missingProbeCategories The probe categories that VMs don't have
         * @param entityType The type of entity to find.
         * @return Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} NOT discovered by a specific type of probe.
         */
        StitchingScope<TopologyEntity> hasAndLacksProbeCategoryEntityTypeStitchingScope(
                @Nonnull final Set<ProbeCategory> owningProbeCategories,
                @Nonnull final Set<ProbeCategory> missingProbeCategories,
                @Nonnull final EntityType entityType);
    }
}
