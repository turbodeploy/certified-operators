package com.vmturbo.cost.component.savings.temold;

import java.lang.reflect.Type;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class responsible for the serialization and deserialization for ProviderInfo objects.
 */
public class ProviderInfoSerializer implements JsonSerializer<ProviderInfo>,
        JsonDeserializer<ProviderInfo> {
    private final ImmutableMap<Integer, Class<? extends BaseProviderInfo>> entityToProviderInfoClass = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, VirtualMachineProviderInfo.class,
            EntityType.VIRTUAL_VOLUME_VALUE, VolumeProviderInfo.class,
            EntityType.DATABASE_VALUE, DatabaseProviderInfo.class,
            EntityType.DATABASE_SERVER_VALUE, DatabaseServerProviderInfo.class
    );

    @Override
    public JsonElement serialize(ProviderInfo providerInfo, Type type,
            JsonSerializationContext context) {
        Class<? extends BaseProviderInfo> providerInfoClass =
                entityToProviderInfoClass.get(providerInfo.getEntityType());
        if (providerInfoClass != null) {
            return context.serialize(providerInfoClass.cast(providerInfo));
        }
        return null;
    }

    @Override
    public ProviderInfo deserialize(JsonElement json, Type type,
            JsonDeserializationContext context) throws JsonParseException {
        JsonElement entityTypeElement = json.getAsJsonObject().get("et");
        if (entityTypeElement != null) {
            Class<? extends BaseProviderInfo> providerInfoClass =
                    entityToProviderInfoClass.get(entityTypeElement.getAsInt());
            if (providerInfoClass != null) {
                return context.deserialize(json, providerInfoClass);
            }
        }
        return null;
    }
}
