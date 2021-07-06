package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Database - i.e. {@link DatabaseInfo}
 **/
public class DatabaseInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasApplicationData() || !sdkEntity.getApplicationData().hasDbData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ApplicationData appData = sdkEntity.getApplicationData();
        final DatabaseData dbData = appData.getDbData();
        Builder appInfo = TypeSpecificInfo.newBuilder();
        if (appData.hasDbData()) {
            // Note that we will add a 'databaseInfo' even if the 'appData.getDbData()' has no info
            final DatabaseInfo.Builder databaseInfoBuilder = DatabaseInfo.newBuilder();
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
            // we don't yet need 'dbData.getVersion() - but that may change
            appInfo.setDatabase(databaseInfoBuilder);
        }
        return appInfo.build();
    }
}
