package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Test the serialization and deserialization of {@link BiMap} objects using
 * the {@link Gson} object produced by {@link ComponentGsonFactory}.
 */
public class GsonBiMapSerializationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test serializing a {@link BiMap} into a JSON string, and deserializing that string
     * back into a {@link BiMap}.
     */
    @Test
    public void testBiMapRoundTrip() {
        final BiMap<String, String> map = HashBiMap.create();
        map.put("foo", "bar");

        final String json = ComponentGsonFactory.createGson().toJson(map);
        final BiMap<String, String> deserializedMap =
                ComponentGsonFactory.createGson().fromJson(json,
                        new TypeToken<BiMap<String, String>>(){}.getType());
        assertThat(deserializedMap, is(map));
    }

    @Test
    public void testBiMapError() {
        final Map<String, String> map = new HashMap<>();
        map.put("foo", "bar");
        map.put("x", "bar");

        final String json = ComponentGsonFactory.createGson().toJson(map);

        expectedException.expect(IllegalArgumentException.class);

        ComponentGsonFactory.createGson().fromJson(json,
            new TypeToken<BiMap<String, String>>() {}.getType());
    }

    @Test
    public void testBiMapNonPrimitive() {
        final BiMap<ValueObj, ValueObj> map = HashBiMap.create();
        map.put(new ValueObj("foo"), new ValueObj("bar"));

        final O json = ComponentGsonFactory.createGson().fromJson("{}", O.class);
        boolean val = true;
    }

    public static class O {
        private BiMap<Integer, String> map;
    }

    /**
     * Simple test object - wrapper around a String.
     */
    private static class ValueObj {
        public final String val;

        public ValueObj(@Nonnull final String val) {
            this.val = val;
        }

        public boolean equals(Object other) {
            return other instanceof ValueObj && ((ValueObj)other).val.equals(val);
        }

        public int hashCode() {
            return Objects.hash(val);
        }
    }
}
