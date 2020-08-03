package com.vmturbo.stitching.utilities;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.DTOFieldSpec;
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
    private static final Logger logger = LogManager.getLogger();

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
                     @Nullable final T fromField,
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
        T mergeField(@Nullable final T fromField, @Nonnull final T ontoField);
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

            // do not set new result on EntityDTO if it is null
            if (mergeResult != null) {
                setter.set(ontoEntity.getEntityBuilder(), mergeResult);
            }
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
     * Return a {@link EntityFieldMerger}  that merges an entity property from the fromEntity onto the
     * ontoEntity.  If the property is empty or doesn't exist, no merge is done.
     *
     * @param propertyName The name of the property to merge.
     * @return An {@link EntityFieldMerger} that merges the named entityProperty
     */
    public static EntityFieldMerger<String> getPropertyFieldMerger(final String propertyName) {
        return merge((entity) -> {
                    return DTOFieldAndPropertyHandler.getPropertyFromEntity(entity, propertyName);
                },
                (entity, newValue) -> {
                    DTOFieldAndPropertyHandler.setPropertyOfEntity(entity, propertyName, newValue);
                })
                .withMethod((fromProperty, ontoProperty) -> fromProperty == null ||
                        fromProperty.isEmpty() ? ontoProperty : fromProperty);
    }

    /**
     * Return an EntityFieldMerger for based on the list of methods in
     * {@link DTOFieldSpec}.
     *
     * @param attribute {@link DTOFieldSpec} encapsulating the getter and setter methods
     *                  for the attribute.
     * @return {@link EntityFieldMerger} representing the getter and setter methods to get and set
     * the passed in attribute.
     */
    public static EntityFieldMerger<Object> getAttributeFieldMerger(
            final DTOFieldSpec attribute) {
        return EntityFieldMergers.merge(
                (entityDTOOrBuilder) -> {
                    try {
                        return DTOFieldAndPropertyHandler.getValueFromFieldSpec(entityDTOOrBuilder,
                                attribute);
                    } catch (NoSuchFieldException e) {
                        logger.error("Could not extract attribute " + attribute.getFieldName()
                        + " from entity " + entityDTOOrBuilder.getDisplayName()
                        + " Exception: " + e);
                        return null;
                    }
                },
                ((builder, newValue) -> {
                    try {
                        DTOFieldAndPropertyHandler.setValueToFieldSpec(builder, attribute, newValue);
                    } catch (NoSuchFieldException e) {
                        logger.error("Could not set value of attribute " + attribute.getFieldName()
                                + " for entity " + builder.getDisplayName()
                                + ".  New value to set was " + newValue
                                + " Exception: " + e);
                    }
                })).withMethod((fromField, ontoField) -> fromField == null ? ontoField : fromField);
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

    private static List<VirtualVolumeFileDescriptor> getFileList(EntityDTOOrBuilder entity) {
        if (entity.hasVirtualVolumeData()) {
            return entity.getVirtualVolumeData().getFileList();
        }
        return Collections.emptyList();
    }

    private static void setFileList(EntityDTO.Builder builder,
                                    List<VirtualVolumeFileDescriptor> files) {
        builder.getVirtualVolumeDataBuilder().clearFile().addAllFile(files);
    }

    /**
     * Return the intersection of two lists of {@link VirtualVolumeFileDescriptor}.
     * @param fromFiles list of file descriptors that come from the 'from' entity.
     * @param ontoFiles list of file descriptors that come from the 'onto' entity.
     * @return list representing the intersection of the two lists that are passed in.
     */
    private static List<VirtualVolumeFileDescriptor> fileListIntersection(
        List<VirtualVolumeFileDescriptor> fromFiles,
        List<VirtualVolumeFileDescriptor> ontoFiles) {
        Set<String> fromPathsSet = fromFiles.stream()
            .map(VirtualVolumeFileDescriptor::getPath)
            .collect(Collectors.toSet());
        return ontoFiles.stream()
            .filter(fileDescriptor -> fromPathsSet.contains(fileDescriptor.getPath()))
            .collect(Collectors.toList());
    }

    /**
     * Merge Files between two virtual volumes representing wasted files for the same shared storage.
     * Because each volume comes from a different target, it is only aware of the file usage for VMs
     * within that target.  So each virtual volume contains files that are not really wasted since
     * they are referened by VMs in the other target.  Merge the two file lists by taking their
     * intersection.
     */
    public static final EntityFieldMerger<List<VirtualVolumeFileDescriptor>>
        VIRTUAL_VOLUME_FILELIST_INTERSECTION =
        merge(EntityFieldMergers::getFileList, EntityFieldMergers::setFileList)
        .withMethod(EntityFieldMergers::fileListIntersection);

}