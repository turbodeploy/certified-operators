package com.vmturbo.stitching.utilities;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.StitchingEntity;

/**
 * Contains helpers and skeleton code for defining methods to merge specific fields
 * when merging entities.
 *
 * These mergers are generally for use in merging simple fields (ie DisplayName, EntityState, etc.)
 * For merging something complex like CommoditiesSold see for example
 * {@link com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy}.
 */
public class EntityFieldMergers {
    /**
     * Get a field from an {@link EntityDTOOrBuilder}.
     * Example {@link EntityDTOOrBuilder#getDisplayName()}
     *
     * @param <T> The type of the field to get.
     */
    @FunctionalInterface
    public interface EntityFieldGetter<T> {
        T get(@Nonnull final EntityDTOOrBuilder entity);
    }

    /**
     * Set a field from an {@link EntityDTO.Builder}.
     * Example {@link EntityDTO.Builder#setDisplayName(String)}
     *
     * @param <T> The type of the field to get.
     */
    @FunctionalInterface
    public interface EntityFieldSetter<T> {
        void set(@Nonnull final EntityDTO.Builder entity,
                 @Nonnull final T newValue);
    }

    /**
     * Merge a field from two {@link StitchingEntity} {@link EntityDTO.Builder}s.
     *
     * @param <T> The type of the field to be merged.
     */
    @FunctionalInterface
    public interface FieldMerger<T> {
        T mergeField(@Nonnull final StitchingEntity fromEntity,
                     @Nonnull final T fromField,
                     @Nonnull final StitchingEntity ontoEntity,
                     @Nonnull final T ontoField);
    }

    /**
     * Merge a field from two {@link StitchingEntity} {@link EntityDTO.Builder}s.
     *
     * A slightly simpler version for when only the fields themselves are needed to
     * decide how to perform the merge.
     *
     * @param <T> The type of the field to be merged.
     */
    @FunctionalInterface
    public interface SimpleFieldMerger<T> {
        T mergeField(@Nonnull final T fromField, @Nonnull final T ontoField);
    }

    /**
     * A skeleton class for defining a means to merge a specific field when merging entities.
     * For use when merging entities using the structures in {@link MergeEntities}.
     *
     * @param <T> The type of the entity field being merged.
     */
    @Immutable
    public static class EntityFieldMerger<T> {
        private final EntityFieldGetter<T> getter;
        private final EntityFieldSetter<T> setter;
        private final FieldMerger<T> fieldMerger;

        /**
         * Create a new {@link EntityFieldMerger} for merging a given field when merging
         * entities.
         *
         * @param getter The getter for getting the field. Should be for the same field as the setter.
         * @param setter The setter for setting the field. Should be for the same field as the getter.
         * @param fieldMerger A method for deciding how to merge a field when merging entities.
         */
        private EntityFieldMerger(@Nonnull final EntityFieldGetter<T> getter,
                                  @Nonnull final EntityFieldSetter<T> setter,
                                  @Nonnull final FieldMerger<T> fieldMerger) {
            this.getter = Objects.requireNonNull(getter);
            this.setter = Objects.requireNonNull(setter);
            this.fieldMerger = Objects.requireNonNull(fieldMerger);
        }

        /**
         * Perform the field merge.
         *
         * @param fromEntity The "from" entity to merge from. For more information on "from" and "onto"
         *                   terminology, see {@link MergeEntities}.
         * @param ontoEntity The "onto" entity to merge onto. For more information on "from" and "onto"
         *                   terminology, see {@link MergeEntities}.
         */
        public void merge(@Nonnull final StitchingEntity fromEntity,
                          @Nonnull final StitchingEntity ontoEntity) {
            final T mergeResult = fieldMerger.mergeField(
                fromEntity, getter.get(fromEntity.getEntityBuilder()),
                ontoEntity, getter.get(ontoEntity.getEntityBuilder())
            );

            setter.set(ontoEntity.getEntityBuilder(), mergeResult);
        }

        /**
         * Get the {@link EntityFieldGetter} for this merger.
         *
         * @return the {@link EntityFieldGetter} for this merger.
         */
        public EntityFieldGetter<T> getGetter() {
            return getter;
        }

        /**
         * Get the {@link EntityFieldSetter} for this merger.
         *
         * @return the {@link EntityFieldSetter} for this merger.
         */
        public EntityFieldSetter<T> getSetter() {
            return setter;
        }
    }

    /**
     * An intermediate builder class for creating a {@link EntityFieldMerger <T>}.
     *
     * @param <T> The type of the field to merge.
     */
    @Immutable
    public static class EntityFieldGetterAndSetter<T> {
        private final EntityFieldGetter<T> getter;
        private final EntityFieldSetter<T> setter;

        private EntityFieldGetterAndSetter(@Nonnull final EntityFieldGetter<T> getter,
                                           @Nonnull final EntityFieldSetter<T> setter) {
            this.getter = Objects.requireNonNull(getter);
            this.setter = Objects.requireNonNull(setter);
        }

        /**
         * Create a new {@link EntityFieldMerger <T>} with the given field merger method.
         * @param fieldMergeMethod The method for merging fields.
         *
         * @return A new {@link EntityFieldMerger<T>} for merging a field on entities.
         */
        public EntityFieldMerger<T> withMethod(@Nonnull final FieldMerger<T> fieldMergeMethod) {
            return new EntityFieldMerger<>(getter, setter, fieldMergeMethod);
        }

        /**
         * Create a new {@link EntityFieldMerger <T>} with the given field merger method.
         * @param fieldMergeMethod The method for merging fields.
         *
         * @return A new {@link EntityFieldMerger<T>} for merging a field on entities.
         */
        public EntityFieldMerger<T> withMethod(@Nonnull final SimpleFieldMerger<T> fieldMergeMethod) {
            return new EntityFieldMerger<>(getter, setter,
                (fromEntity, fromField, ontoEntity, ontoField) ->
                    fieldMergeMethod.mergeField(fromField, ontoField));
        }
    }

    /**
     * A static factory method for generating an {@link EntityFieldGetterAndSetter <T>} which can be used to
     * create a {@link EntityFieldMerger <T>}.
     *
     * @param getter Gets the field to merge from an entity.
     * @param setter Sets the field to merge on an entity.
     * @param <T> The type of the field to merge.
     * @return An {@link EntityFieldMerger <T>}.
     */
    public static <T> EntityFieldGetterAndSetter<T> merge(@Nonnull final EntityFieldGetter<T> getter,
                                                             @Nonnull final EntityFieldSetter<T> setter) {
        return new EntityFieldGetterAndSetter<>(getter, setter);
    }

    /**
     * Merge the display names for two entities, keeping the one that's lexicographically first.
     *
     * It's important to choose a consistent display name when merging entities because, for
     * two targets discovering the same entity (ie a shared storage) have been configured
     * with different displayNames for the entity and use this heuristic for merging
     * displayName, the displayName can potentially ping-pong between the two even if using
     * the heuristic to pick the displayName from the entity discovered more recently.
     *
     * This results in very confusing behavior. So to ensure a consistent display name when
     * the set of targets is consistent, we can use a special heuristic for merging displayNames -
     * that is, always pick the displayName that is lexicographically first no matter which
     * was discovered more recently.
     */
    public static final EntityFieldMerger<String> DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST =
        merge(EntityDTOOrBuilder::getDisplayName, EntityDTO.Builder::setDisplayName)
        .withMethod((fromDisplayName, ontoDisplayName) ->
            fromDisplayName.compareTo(ontoDisplayName) < 0 ? fromDisplayName : ontoDisplayName);
}