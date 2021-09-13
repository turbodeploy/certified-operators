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
        Builder appInfo = TypeSpecificInfo.newBuilder();
        if (appData.hasDbData()) {
            final DatabaseData dbData = appData.getDbData();
            // Note that we will add a 'databaseInfo' even if the 'appData.getDbData()' has no info
            final DatabaseInfo.Builder databaseInfoBuilder = DatabaseInfo.newBuilder();

            setupDatabaseData(dbData, databaseInfoBuilder);

            appInfo.setDatabase(databaseInfoBuilder);
        }
        return appInfo.build();
    }
}
