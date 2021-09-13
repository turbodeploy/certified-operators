package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Abstract class for individual mappers from {@link EntityDTO} information to type-specific
 * information for that particular EntityType.
 */
public abstract class TypeSpecificInfoMapper {
    private static final Logger logger = LogManager.getLogger();

    private static final Map<String, CloudCostDTO.DeploymentType> DEPLOYMENT_TYPE_MAP =
        ImmutableMap.<String, CloudCostDTO.DeploymentType>builder()
        .put("MultiAz", CloudCostDTO.DeploymentType.MULTI_AZ)
        .put("SingleAz", CloudCostDTO.DeploymentType.SINGLE_AZ).build();

    private static final Map<String, CloudCostDTO.LicenseModel> LICENSE_MODEL_MAP = ImmutableMap.<String, CloudCostDTO.LicenseModel>builder()
        .put("BringYourOwnLicense", CloudCostDTO.LicenseModel.BRING_YOUR_OWN_LICENSE)
        .put("LicenseIncluded", CloudCostDTO.LicenseModel.LICENSE_INCLUDED)
        .put("NoLicenseRequired", CloudCostDTO.LicenseModel.NO_LICENSE_REQUIRED).build();


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

    @Nonnull
    public abstract TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap);

    /**
     * Create a {@link TypeSpecificInfo} with the 'oneof' field corresponding to the type
     * of the given {@link EntityDTO}.
     *
     * @param entity probe entity DTO.
     * @param sdkEntity the SDK {@link EntityDTO} for which we will build the {@link TypeSpecificInfo}
     * @param  entityPropertyMap the mapping from property name to property value, which comes from
     * the {@link EntityDTO#entityProperties_}. For most cases, the type specific info is set in
     * {@link EntityDTO#entityData_}, but some are only set inside {@link EntityDTO#entityProperties_}
     * @return a new {@link TypeSpecificInfo} with the 'oneof' field corresponding to the type
     * of the given 'sdkEntity'
     */
    @Nonnull
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nullable TopologyStitchingEntity entity,
            @Nonnull EntityDTOOrBuilder sdkEntity, @Nonnull Map<String, String> entityPropertyMap) {
        return mapEntityDtoToTypeSpecificInfo(sdkEntity, entityPropertyMap);
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
     * then return {@link DatabaseEngine#UNKNOWN}. To check string representation
     * {@link com.vmturbo.mediation.util.DatabaseEngine} is used.
     *
     * @param dbEngine a string representing the Database Engine
     * @return the {@link DatabaseEngine} enum value corresponding to the given string, or
     * DatabaseEngine.UNKNOWN of not found.
     */
    @Nonnull
    protected DatabaseEngine parseDbEngine(@Nonnull final String dbEngine) {
        Optional<com.vmturbo.mediation.util.DatabaseEngine> databaseEngine
                = com.vmturbo.mediation.util.DatabaseEngine.getBy(dbEngine);
        if (databaseEngine.isPresent()) {
            try {
                return DatabaseEngine.valueOf(String.valueOf(databaseEngine.get()).toUpperCase());
            } catch (IllegalArgumentException e) {
                return DatabaseEngine.UNKNOWN;
            }
        } else {
            return DatabaseEngine.UNKNOWN;
        }
    }

    /**
     * Convert a string representation of the Deployment type to the corresponding
     * {@link CloudCostDTO.DeploymentType} enum value. If no corresponding enum value can be found,
     * then return empty.
     *
     * @param deploymentType a string representing the Deployment type
     * @return the {@link CloudCostDTO.DeploymentType} enum value corresponding to the given string, or
     * empty if not found.
     */
    protected Optional<CloudCostDTO.DeploymentType> parseDeploymentType(@Nonnull final String deploymentType) {
        CloudCostDTO.DeploymentType mappedDeploymentType = DEPLOYMENT_TYPE_MAP.get(deploymentType);

        if (mappedDeploymentType == null) {
            logger.warn("The provided deployment type {} is not supported.", deploymentType);
        }

        return Optional.ofNullable(mappedDeploymentType);
    }

    /**
     * Convert a string representation of the License model to the corresponding
     * {@link CloudCostDTO.LicenseModel} enum value. If no corresponding enum value can be found,
     * then return empty.
     *
     * @param licenseModel a string representing the License model
     * @return the {@link CloudCostDTO.LicenseModel} enum value corresponding to the given string, or
     * empty if not found.
     */
    protected Optional<CloudCostDTO.LicenseModel> parseLicenseModel(@Nonnull final String licenseModel) {
        CloudCostDTO.LicenseModel mappedLicenseModel = LICENSE_MODEL_MAP.get(licenseModel);

        if (mappedLicenseModel == null) {
            logger.warn("The provided license model {} is not supported.", licenseModel);
        }

        return Optional.ofNullable(mappedLicenseModel);
    }

    protected void setupDatabaseData(DatabaseData dbData, DatabaseInfo.Builder databaseInfoBuilder) {
        if (dbData.hasEdition()) {
            databaseInfoBuilder.setEdition(parseDbEdition(dbData.getEdition()));
            databaseInfoBuilder.setRawEdition(dbData.getEdition());
        }
        if (dbData.hasEngine()) {
            databaseInfoBuilder.setEngine(parseDbEngine(dbData.getEngine()));
        }
        if (dbData.hasVersion()) {
            databaseInfoBuilder.setVersion(dbData.getVersion());
        }
        if (dbData.hasDeploymentType()) {
            parseDeploymentType(dbData.getDeploymentType()).ifPresent(
                databaseInfoBuilder::setDeploymentType);
        }
        if (dbData.hasLicenseModel()) {
            parseLicenseModel(dbData.getLicenseModel()).ifPresent(
                databaseInfoBuilder::setLicenseModel);
        }

        if (!dbData.getLowerBoundScaleUpList().isEmpty()) {
            databaseInfoBuilder.addAllLowerBoundScaleUp(dbData.getLowerBoundScaleUpList());
        }
        if (dbData.hasHourlyBilledOps()) {
            databaseInfoBuilder.setHourlyBilledOps(dbData.getHourlyBilledOps());
        }

    }

    protected void setupDatabaseServerData(DatabaseServerData dbServerData, DatabaseServerInfo.Builder databaseServerInfoBuilder) {
        if (dbServerData.hasEdition()) {
            databaseServerInfoBuilder.setEdition(parseDbEdition(dbServerData.getEdition()));
            databaseServerInfoBuilder.setRawEdition(dbServerData.getEdition());
        }
        if (dbServerData.hasEngine()) {
            databaseServerInfoBuilder.setEngine(parseDbEngine(dbServerData.getEngine()));
        }
        if (dbServerData.hasVersion()) {
            databaseServerInfoBuilder.setVersion(dbServerData.getVersion());
        }
        if (dbServerData.hasDeploymentType()) {
            parseDeploymentType(dbServerData.getDeploymentType()).ifPresent(
                databaseServerInfoBuilder::setDeploymentType);
        }
        if (dbServerData.hasLicenseModel()) {
            parseLicenseModel(dbServerData.getLicenseModel()).ifPresent(
                databaseServerInfoBuilder::setLicenseModel);
        }
        if (!dbServerData.getLowerBoundScaleUpList().isEmpty()) {
            databaseServerInfoBuilder.addAllLowerBoundScaleUp(dbServerData.getLowerBoundScaleUpList());
        }
        if (dbServerData.hasHourlyBilledOps()) {
            databaseServerInfoBuilder.setHourlyBilledOps(dbServerData.getHourlyBilledOps());
        }
    }

    protected void setupDatabaseServerData(DatabaseData dbServerData, DatabaseServerInfo.Builder databaseServerInfoBuilder) {
        if (dbServerData.hasEdition()) {
            databaseServerInfoBuilder.setEdition(parseDbEdition(dbServerData.getEdition()));
            databaseServerInfoBuilder.setRawEdition(dbServerData.getEdition());
        }
        if (dbServerData.hasEngine()) {
            databaseServerInfoBuilder.setEngine(parseDbEngine(dbServerData.getEngine()));
        }
        if (dbServerData.hasVersion()) {
            databaseServerInfoBuilder.setVersion(dbServerData.getVersion());
        }
        if (dbServerData.hasDeploymentType()) {
            parseDeploymentType(dbServerData.getDeploymentType()).ifPresent(
                databaseServerInfoBuilder::setDeploymentType);
        }
        if (dbServerData.hasLicenseModel()) {
            parseLicenseModel(dbServerData.getLicenseModel()).ifPresent(
                databaseServerInfoBuilder::setLicenseModel);
        }
        if (!dbServerData.getLowerBoundScaleUpList().isEmpty()) {
            databaseServerInfoBuilder.addAllLowerBoundScaleUp(dbServerData.getLowerBoundScaleUpList());
        }
        if (dbServerData.hasHourlyBilledOps()) {
            databaseServerInfoBuilder.setHourlyBilledOps(dbServerData.getHourlyBilledOps());
        }
    }

}
