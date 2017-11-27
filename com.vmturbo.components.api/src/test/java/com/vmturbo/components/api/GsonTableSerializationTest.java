package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

/**
 * Test {@link Table} serialization with the {@link Gson} object created by
 * {@link ComponentGsonFactory}.
 */
public class GsonTableSerializationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test converting a {@link Table} object to JSON and back.
     */
    @Test
    public void testTableRoundTrip() {
        final Table<String, String, ValueObj> table = HashBasedTable.create();
        table.put("row", "col", new ValueObj("value"));

        final Gson gson = ComponentGsonFactory.createGson();
        String json = gson.toJson(table);
        final Table<String, String, ValueObj> retTable =
                gson.fromJson(json, new TypeToken<Table<String, String, ValueObj>>(){}.getType());
        assertThat(retTable, is(table));
    }

    @Test
    public void testTableIllegalColType() {
        final Table<String, ValueObj, ValueObj> table = HashBasedTable.create();
        table.put("row", new ValueObj("value"), new ValueObj("value"));
        expectedException.expect(IllegalArgumentException.class);
        ComponentGsonFactory.createGson().toJson(table);
    }

    @Test
    public void testTableIllegalRowType() {
        final Table<ValueObj, String, ValueObj> table = HashBasedTable.create();
        table.put(new ValueObj("row"), "value", new ValueObj("value"));
        expectedException.expect(IllegalArgumentException.class);
        ComponentGsonFactory.createGson().toJson(table);
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
