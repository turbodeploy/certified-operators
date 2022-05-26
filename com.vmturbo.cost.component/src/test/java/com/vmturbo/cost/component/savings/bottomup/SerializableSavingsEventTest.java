package com.vmturbo.cost.component.savings.bottomup;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableActionEvent;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableEntityPriceChange;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableTopologyEvent;
import com.vmturbo.cost.component.savings.temold.DatabaseProviderInfo;
import com.vmturbo.cost.component.savings.temold.DatabaseServerProviderInfo;
import com.vmturbo.cost.component.savings.temold.ProviderInfo;
import com.vmturbo.cost.component.savings.temold.ProviderInfoSerializer;
import com.vmturbo.cost.component.savings.temold.VirtualMachineProviderInfo;
import com.vmturbo.cost.component.savings.temold.VolumeProviderInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;

public class SerializableSavingsEventTest {
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory(new GsonAdaptersSerializableSavingsEvent())
            .registerTypeAdapter(ProviderInfo.class, new ProviderInfoSerializer())
            .create();

    @Test
    public void serializeActionEvent() {
        final SerializableSavingsEvent event = new SerializableSavingsEvent.Builder()
                .expirationTime(101L)
                .actionEvent(new SerializableActionEvent.Builder()
                        .actionId(144703638357808L)
                        .actionType(ActionType.SCALE_VALUE)
                        .actionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT_VALUE)
                        .build())
                .entityPriceChange(new SerializableEntityPriceChange.Builder()
                        .sourceCost(10.5d)
                        .destinationCost(20.6d)
                        .sourceOid(74307078255731L)
                        .destinationOid(401L)
                        .active(1)
                        .build())
                .build();
        /*
Serialized String will look like this:
{
    "ae": {
        "ac": 2,
        "aid": 144703638357808,
        "at": 11
    },
    "eid": 101,
    "pc": {
        "dc": 20.6,
        "do": 401,
        "sc": 10.5,
        "so": 74307078255731,
        "act": 1
    }
}
         */
        verifyEvent(event);
    }

    @Test
    public void serializeTopologyEvents() {
        Map<Integer, Double> volCommodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                CommodityType.IO_THROUGHPUT_VALUE, 200d,
                CommodityType.STORAGE_ACCESS_VALUE, 300d);
        Map<Integer, Double> dbCommodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d);
        List<ProviderInfo> providerInfoList = Arrays.asList(
                new VirtualMachineProviderInfo(1000L),
                new VolumeProviderInfo(1001L, volCommodityMap),
                new DatabaseProviderInfo(1002L, dbCommodityMap),
                new DatabaseServerProviderInfo(1002L, dbCommodityMap, DeploymentType.SINGLE_AZ)
        );
        providerInfoList.forEach(this::serializeTopologyEvent);
    }

    public void serializeTopologyEvent(ProviderInfo providerInfo) {
        final SerializableSavingsEvent event = new SerializableSavingsEvent.Builder()
                .expirationTime(101L)
                .topologyEvent(new SerializableTopologyEvent.Builder()
                        .entityRemoved(false)
                        .providerInfo(providerInfo)
                        .build())
                .entityPriceChange(new SerializableEntityPriceChange.Builder()
                        .sourceCost(0.026999998745852953d)
                        .destinationCost(0.007219178056063718d)
                        .sourceOid(74307078255731L)
                        .destinationOid(401L)
                        .active(0)
                        .build())
                .build();
        /*
Serialized string will look like this:
{
    "eid": 101,
    "pc": {
        "dc": 0.007219178056063718,
        "do": 401,
        "sc": 0.026999998745852953,
        "so": 74307078255731,
        "act": 0
    },
    "te": {
        "cu": {
            "39": 61440.0,
            "64": 500.0,
            "8": 65536.0
        },
        "er": false,
        "tid": 778177499166655
    }
}
         */
        verifyEvent(event);
    }

    private void verifyEvent(final SerializableSavingsEvent eventWritten) {
        final String jsonWritten = gson.toJson(eventWritten);
        final SerializableSavingsEvent eventRead = gson.fromJson(jsonWritten,
                SerializableSavingsEvent.class);
        assertEquals(eventWritten, eventRead);
        final String jsonRead = gson.toJson(eventRead);
        assertEquals(jsonWritten, jsonRead);
    }
}
