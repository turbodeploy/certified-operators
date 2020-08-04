package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to an Application - i.e. {@link ApplicationInfo}.
 * Further, if this Application is a DB Application, populate the {@link DatabaseInfo} as well.
 **/
public class ApplicationInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasApplicationData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ApplicationData appData = sdkEntity.getApplicationData();
        final ApplicationInfo.Builder applicationInfoBuilder = ApplicationInfo.newBuilder();
        if (appData.hasIpAddress()) {
            applicationInfoBuilder.setIpAddress(IpAddress.newBuilder()
                //  TODO: how do we determine if an application or db ip is elastic?
                .setIpAddress(appData.getIpAddress())
            );
        }
        final TypeSpecificInfo.Builder appInfo = TypeSpecificInfo.newBuilder()
                .setApplication(applicationInfoBuilder);
        // if this is a DB application, then populate the DB Info as well
        if (appData.hasDbData()) {
            final DatabaseData dbData = appData.getDbData();
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
            // we don't yet need 'dbData.getVersion() - but that may change
            appInfo.setDatabase(databaseInfoBuilder);
        }
        return appInfo.build();
    }
}
