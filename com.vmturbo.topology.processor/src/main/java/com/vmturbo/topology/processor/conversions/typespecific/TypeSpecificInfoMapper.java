package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

/**
 * Abstract class for individual mappers from {@link EntityDTO} information to type-specific
 * information for that particular EntityType.
 */
public abstract class TypeSpecificInfoMapper {

    /**
     * Create a {@link TypeSpecificInfo} with the 'oneof' field corresponding to the type
     * of the given {@link EntityDTO}.
     *
     * @param sdkEntity the SDK {@link EntityDTO} for which we will build the {@link TypeSpecificInfo}
     * @return a new {@link TypeSpecificInfo} with the 'oneof' field corresponding to the type
     * of the given 'sdkEntity'
     */
    public abstract TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(EntityDTOOrBuilder sdkEntity);

    /**
     * Fetch an Optional containing the value of EntityProperty with the given name
     * from the getEntityPropertiesList of the given {@link EntityDTO} if found,
     * or Optional.empty() if not found.
     * <p/>
     * Note that we ignore the namespace.
     *
     * @param sdkEntity the {@link EntityDTO} to look at
     * @param propertyName the name of the property to fetch from 'entityPropertiesList'
     * @return Optional containing the value of the property with the given name,
     * or Optional.empty() if not found
     */
    protected Optional<String> getEntityPropertyValue(@Nonnull EntityDTOOrBuilder sdkEntity,
                                                                      @Nonnull String propertyName) {
        Objects.requireNonNull(propertyName);
        return Objects.requireNonNull(sdkEntity).getEntityPropertiesList().stream()
            .filter(entityProperty -> entityProperty.getName().equals(propertyName))
            .map(EntityProperty::getValue)
            .findFirst();
    }

    /**
     * Convert a string representation of the Database Edition to the corresponding
     * {@link DatabaseEdition} enum value. If no corresponding enum value can be found,
     * then return {@link DatabaseEdition#NONE}.
     *
     * @param dbEdition a string representing the Database Edition
     * @return the {@link DatabaseEdition} enum value corresponding to the given string, or
     * DatabaseEdition.NONE of not found.
     */
    @Nonnull
    protected DatabaseEdition parseDbEdition(@Nonnull final String dbEdition) {
        final String upperCaseDbEdition = dbEdition.toUpperCase();
        try {
            return DatabaseEdition.valueOf(upperCaseDbEdition);
        } catch (IllegalArgumentException e) {
            return DatabaseEdition.NONE;
        }
    }

    /**
     * Convert a string representation of the Database Engine to the corresponding
     * {@link DatabaseEngine} enum value. If no corresponding enum value can be found,
     * then return {@link DatabaseEngine#UNKNOWN}.
     *
     * @param dbEngine a string representing the Database Engine
     * @return the {@link DatabaseEngine} enum value corresponding to the given string, or
     * DatabaseEngine.UNIKNOWN of not found.
     */
    @Nonnull
    protected DatabaseEngine parseDbEngine(@Nonnull final String dbEngine) {
        final String upperCaseDbEngine = dbEngine.toUpperCase();
        try {
            return DatabaseEngine.valueOf(upperCaseDbEngine);
        } catch (IllegalArgumentException e) {
            return DatabaseEngine.UNKNOWN;
        }
    }


}
