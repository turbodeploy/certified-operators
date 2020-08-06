package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
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
        return TypeSpecificInfo.newBuilder()
            .setDatabase(DatabaseInfo.newBuilder()
                .setEdition(parseDbEdition(dbData.getEdition()))
                .setRawEdition(dbData.getEdition())
                .setEngine(parseDbEngine(dbData.getEngine()))
                .setVersion(dbData.getVersion())
                .build())
            .build();
    }
}
