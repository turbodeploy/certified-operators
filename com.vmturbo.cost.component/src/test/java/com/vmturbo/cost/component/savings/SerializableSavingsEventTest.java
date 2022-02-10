package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.cost.component.savings.SerializableSavingsEvent.SerializableActionEvent;
import com.vmturbo.cost.component.savings.SerializableSavingsEvent.SerializableEntityPriceChange;
import com.vmturbo.cost.component.savings.SerializableSavingsEvent.SerializableTopologyEvent;

public class SerializableSavingsEventTest {
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory(new GsonAdaptersSerializableSavingsEvent())
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
    public void serializeTopologyEvent() {
        final SerializableSavingsEvent event = new SerializableSavingsEvent.Builder()
                .expirationTime(101L)
                .topologyEvent(new SerializableTopologyEvent.Builder()
                        .providerOid(778177499166655L)
                        .commodityUsage(ImmutableMap.of(
                                64, 500.000000d,
                                39, 61440.000000d,
                                8, 65536.000000
                        ))
                        .entityRemoved(false)
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
