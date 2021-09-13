package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a DatabaseServerInfo - i.e. {@link DatabaseServerInfo}
 **/
public class DatabaseServerInfoMapper extends TypeSpecificInfoMapper {

    /**
     * Map a given entity dto to its type specific information.
     *
     * @param sdkEntity         the SDK {@link EntityDTO} for which we will build the {@link TypeSpecificInfo}
     * @param entityPropertyMap the mapping from property name to property value, which comes from
     *                          the {@link EntityDTO#entityProperties_}. For most cases, the type specific info is set in
     *                          {@link EntityDTO#entityData_}, but some are only set inside {@link EntityDTO#entityProperties_}
     * @return TypeSpecificInfo for the given sdk entity
     */
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
        @Nonnull final EntityDTOOrBuilder sdkEntity,
        @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasApplicationData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ApplicationData appData = sdkEntity.getApplicationData();
        final Builder appInfo = TypeSpecificInfo.newBuilder();

        // if this is a DB application, then populate the DB Info as well
        if (appData.hasDbServerData()) {
            final DatabaseServerData dbServerData = appData.getDbServerData();
            // Note that we will add a 'databaseServerInfo' even if the 'appData.getDbServerData()' has no info
            final DatabaseServerInfo.Builder databaseServerInfoBuilder = DatabaseServerInfo.newBuilder();

            setupDatabaseServerData(dbServerData, databaseServerInfoBuilder);

            appInfo.setDatabaseServer(databaseServerInfoBuilder);
        } else if (appData.hasDbData()) {
            // For backward compatibility reasons with IWO, DatabaseServer attributes
            // may exist in DbData
            final DatabaseData dbData = appData.getDbData();
            // Note that we will add a 'databaseInfo' even if the 'appData.getDbData()' has no info
            final DatabaseServerInfo.Builder databaseServerInfoBuilder = DatabaseServerInfo.newBuilder();

            setupDatabaseServerData(dbData, databaseServerInfoBuilder);

            appInfo.setDatabaseServer(databaseServerInfoBuilder);
        }
        return appInfo.build();
    }

}
