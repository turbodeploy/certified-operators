package com.vmturbo.components.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.StringJoiner;

import org.junit.Test;

public class StringsTest {

    @Test
    public void concat_should_concat_two_or_more_strings_and_exclude_nulls() {
        assertEquals("foobar", Strings.concat("foo", null, "bar"));
    }

    @Test
    public void toString_should_call_toString_on_the_argument() {
        assertEquals("123", Strings.toString(123));
        assertEquals("123.4", Strings.toString(123.4));
        assertEquals("1234567.8", Strings.toString(1234567.8));
        assertEquals("[1, 2, 3, 4]", Strings.toString(Arrays.asList(1, 2, 3, 4)));
        assertEquals("true", Strings.toString(true));
        assertEquals("SomeClass[someField='foo']", Strings.toString(new SomeClass().setSomeField("foo")));
    }

    @Test
    public void toString_should_return_empty_string_for_null_argument() {
        assertEquals("", Strings.toString(null));
    }

    @Test
    public void parseInteger_should_parse_numeric_strings_to_integer() {
        assertEquals(1234, Strings.parseInteger("1234").intValue());
        assertEquals(1234, Strings.parseInteger("   1234   ").intValue());
        assertNull(Strings.parseInteger("123.4"));
        assertNull(Strings.parseInteger("abc"));
        assertNull(Strings.parseInteger(""));
        assertNull(Strings.parseInteger(null));
        assertEquals(1234, Strings.parseInteger("abc", 1234).intValue());
    }

    private static class SomeClass {
        private String someField;

        public String getSomeField() {
            return someField;
        }

        public SomeClass setSomeField(final String someField) {
            this.someField = someField;
            return this;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SomeClass.class.getSimpleName() + "[", "]")
                    .add("someField='" + someField + "'")
                    .toString();
        }
    }
}