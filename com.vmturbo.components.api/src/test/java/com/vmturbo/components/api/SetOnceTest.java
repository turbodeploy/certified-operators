package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class SetOnceTest {

    @Test
    public void trySetValue() {
        SetOnce<Integer> setOnce = new SetOnce<>();
        setOnce.trySetValue(() -> 1 + 1);
        assertThat(setOnce.getValue().get(), is(2));
    }

    @Test
    public void trySetValue1() {
    }
}