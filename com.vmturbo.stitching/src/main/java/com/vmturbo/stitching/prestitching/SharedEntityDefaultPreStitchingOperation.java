package com.vmturbo.stitching.prestitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.EntityScopeFilters;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * Default pre stitching operation for shared entities.
 */
public class SharedEntityDefaultPreStitchingOperation implements PreStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    private final Function<StitchingScopeFactory<StitchingEntity>, StitchingScope<StitchingEntity>>
            scopeGetter;
    private final Map<String, Comparator<Object>> customIdentityFunctions = new HashMap<>();

    /**
     * Constructor.
     *
     * @param scopeGetter function to get the scope for this pre stitching operation
     * associated with custom identity functions.
     */
    public SharedEntityDefaultPreStitchingOperation(
            final Function<StitchingScopeFactory<StitchingEntity>, StitchingScope<StitchingEntity>> scopeGetter) {
        this(scopeGetter, Collections.emptyMap());
    }

    /**
     * Constructor.
     *
     * @param scopeGetter function to get the scope for this pre stitching operation
     * @param customIdentityFunctions the full field name is used as a map key whose value is
     * associated with custom identity functions.
     */
    public SharedEntityDefaultPreStitchingOperation(
            final Function<StitchingScopeFactory<StitchingEntity>, StitchingScope<StitchingEntity>> scopeGetter,
            final Map<String, Comparator<Object>> customIdentityFunctions) {
        this.scopeGetter = scopeGetter;
        // Keep the display name for the entity alphabetically first to prevent ping-ponging.
        this.customIdentityFunctions.put("common_dto.EntityDTO.displayName",
                (lhs, rhs) -> ((String)rhs).compareTo((String)lhs));
        this.customIdentityFunctions.putAll(customIdentityFunctions);
    }

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return scopeGetter.apply(stitchingScopeFactory);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
            @Nonnull final Stream<StitchingEntity> entities,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        EntityScopeFilters.sharedEntitiesByOid(entities).forEach(sharedEntities -> {
            // Merge into the most recently updated entity.
            sharedEntities.stream()
                    .max(Comparator.comparing(StitchingEntity::getLastUpdatedTime))
                    .ifPresent(entityToKeep -> {
                        final List<StitchingEntity> duplicateEntities = sharedEntities.stream()
                                .filter(duplicateEntity -> duplicateEntity != entityToKeep)
                                .collect(Collectors.toList());
                        mergeSharedEntities(duplicateEntities, entityToKeep, resultBuilder);
                    });
        });
        return resultBuilder.build();
    }

    protected void mergeSharedEntities(@Nonnull final List<StitchingEntity> sources,
            @Nonnull final StitchingEntity destination,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        sources.forEach(source -> mergeSharedEntities(source, destination, resultBuilder));
    }

    protected void mergeSharedEntities(@Nonnull final StitchingEntity source,
            @Nonnull final StitchingEntity destination,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueChangeRelationships(destination,
                dst -> CopyCommodities.copyCommodities().from(source).to(dst));
        resultBuilder.queueUpdateEntityAlone(destination,
                dst -> mergeMessageBuilders(source.getEntityBuilder(), dst.getEntityBuilder()));
        resultBuilder.queueEntityMerger(MergeEntities.mergeEntity(source).onto(destination));
    }

    /**
     * Merge the source message into the destination message.
     * Default behaviour when merge collection: add entries with different identities to the
     * results.
     * Default behaviour when merge singleton composite: merge all non-key fields recursively.
     * Default behaviour when merge singleton primitive: choose any.
     *
     * @param source from {@link Message.Builder}
     * @param destination to {@link Message.Builder}
     */
    private void mergeMessageBuilders(final Message.Builder source,
            final Message.Builder destination) {
        if (source.getDescriptorForType() != destination.getDescriptorForType()) {
            throw new IllegalArgumentException(
                    String.format("Only messages of the same type can be merged: %s != %s.",
                            source.getDescriptorForType().getFullName(),
                            destination.getDescriptorForType().getFullName()));
        }
        logger.trace("Merge the source message into the destination message: {} -> {}",
                source::toString, destination::toString);
        source.getAllFields().forEach((fieldDescriptor, sourceFieldValue) -> {
            if (fieldDescriptor.isMapField()) {
                for (Object entry : (List<?>)sourceFieldValue) {
                    destination.addRepeatedField(fieldDescriptor, entry);
                }
            } else if (fieldDescriptor.isRepeated()) {
                // Merge collection: add entries with different identities to the results.
                final List<?> destinationFieldValue =
                        (List<?>)destination.getField(fieldDescriptor);
                final Comparator<Object> identityFunction =
                        customIdentityFunctions.get(fieldDescriptor.getFullName());
                final Set<Object> newRepeatedFieldValue =
                        identityFunction != null ? new TreeSet<>(identityFunction) :
                                new HashSet<>();
                newRepeatedFieldValue.addAll((List<?>)sourceFieldValue);
                newRepeatedFieldValue.addAll(destinationFieldValue);
                destination.setField(fieldDescriptor, new ArrayList<>(newRepeatedFieldValue));
            } else if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                final Message destinationExistingFieldValue =
                        (Message)destination.getField(fieldDescriptor);
                // Merge singleton composite: merge all non-key fields recursively.
                if (destinationExistingFieldValue ==
                        destinationExistingFieldValue.getDefaultInstanceForType()) {
                    destination.setField(fieldDescriptor, sourceFieldValue);
                } else {
                    final Message.Builder existingValueBuilder =
                            destinationExistingFieldValue.toBuilder();
                    mergeMessageBuilders(((Message)sourceFieldValue).toBuilder(),
                            existingValueBuilder);
                    destination.setField(fieldDescriptor, existingValueBuilder.build());
                }
            } else {
                // Merge singleton primitive: choose any.
                if (destination.hasField(fieldDescriptor)) {
                    final Comparator<Object> identityFunction =
                            customIdentityFunctions.get(fieldDescriptor.getFullName());
                    if (identityFunction != null && identityFunction.compare(sourceFieldValue,
                            destination.getField(fieldDescriptor)) > 0) {
                        destination.setField(fieldDescriptor, sourceFieldValue);
                    }
                } else {
                    destination.setField(fieldDescriptor, sourceFieldValue);
                }
            }
            logger.trace("Destination message after merge field {}: {}",
                    fieldDescriptor::getFullName, destination::toString);
        });
    }
}
