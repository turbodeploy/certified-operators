package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
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
     * @param  entityPropertyMap the mapping from property name to property value, which comes from
     * the {@link EntityDTO#entityProperties_}. For most cases, the type specific info is set in
     * {@link EntityDTO#entityData_}, but some are only set inside {@link EntityDTO#entityProperties_}
     * @return a new {@link TypeSpecificInfo} with the 'oneof' field corresponding to the type
     * of the given 'sdkEntity'
     */
    public abstract TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap);

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
