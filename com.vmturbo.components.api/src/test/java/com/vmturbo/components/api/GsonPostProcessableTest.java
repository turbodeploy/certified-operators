package com.vmturbo.components.api;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * A test to verify that the {@link GsonPostProcessable} interface works as expected
 * with {@link ComponentGsonFactory}.
 */
public class GsonPostProcessableTest {

    @Test
    public void testPostProcessDeserialize() {
        Gson gson = ComponentGsonFactory.createGson();
        PostProcessObj obj = gson.fromJson("{\"actualStr\":\"Hello\"}", PostProcessObj.class);
        assertEquals("Hello", obj.getActualStr());
        assertEquals(StringUtils.reverse(obj.getActualStr()), obj.getReversedStr());
    }

    /**
     * Test to verify that the post-process support doesn't get in the way of deserializing
     * normal objects.
     */
    @Test
    public void testNoPostProcess() {
        Gson gson = ComponentGsonFactory.createGson();
        NoPostProcessObj obj = gson.fromJson("{\"actualStr\":\"Hello\"}", NoPostProcessObj.class);
        assertEquals("Hello", obj.getActualStr());
    }

    static class PostProcessObj implements GsonPostProcessable {
        private final String actualStr;

        private transient String reversedStr;

        private PostProcessObj() {
            actualStr = "";
        }

        @Override
        public void postDeserialize() {
            this.reversedStr = StringUtils.reverse(actualStr);
        }

        String getActualStr() {
            return actualStr;
        }

        String getReversedStr() {
            return reversedStr;
        }
    }

    static class NoPostProcessObj {
        private final String actualStr;

        private NoPostProcessObj() {
            actualStr = "";
        }

        String getActualStr() {
            return actualStr;
        }
    }
}
