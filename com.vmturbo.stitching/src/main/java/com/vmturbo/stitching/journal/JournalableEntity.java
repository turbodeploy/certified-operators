package com.vmturbo.stitching.journal;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * State changes to an object implementing this interface can be tracked via a stitching journal.
 *
 * A StitchingJournal is a journal to record changes to the topology made during various stitching phases.
 */
public interface JournalableEntity<T extends JournalableEntity> {
    /**
     * Capture a snapshot of this object that can be diffed with a version of this object with a different state.
     * The difference will be recorded to a journal tracking changes.
     *
     * The snapshot must contain a deep copy of the original object so that changes to any state associated
     * with the original {@link JournalableEntity} do not affect the snapshot version.
     *
     * @return A snapshot of this {@link JournalableEntity}.
     */
    @Nonnull
    T snapshot();

    /**
     * Get the OID (Object ID) of the {@link JournalableEntity}.
     *
     * @return The OID (Object ID) of the {@link JournalableEntity}.
     */
    long getOid();

    /**
     * Get the {@link EntityType} of this {@link JournalableEntity}.
     *
     * @return the {@link EntityType} of this {@link JournalableEntity}.
     */
    @Nonnull
    EntityType getJournalableEntityType();

    /**
     * Get the display name for the entity.
     *
     * @return the display name for the entity.
     */
    @Nonnull
    String getDisplayName();

    /**
     * Get the ids of the targets that discovered this entity.
     * If the entity was not discovered, (ie it is created due to workload added
     * by a plan, etc.), this method should return an empty stream.
     *
     * @return
     */
    @Nonnull
    Stream<Long> getDiscoveringTargetIds();

    /**
     * Get a signature for the entity. Note that this signature is not necessarily
     * unique for every entity in the topology. It is intended more for providing
     * readability and easy searchability for the entity in the journal.
     *
     * @return a string containing the entity signature.
     */
    @Nonnull
    default String getJournalableSignature() {
        return getJournalableEntityType() + "-" + getOid() + "-" + getDisplayName();
    }

    /**
     * Compose a string describing the removal of this {@link JournalableEntity} from the topology.
     *
     * @return A string describing the removal of this {@link JournalableEntity} from the topology.
     */
    @Nonnull
    String removalDescription();
}
