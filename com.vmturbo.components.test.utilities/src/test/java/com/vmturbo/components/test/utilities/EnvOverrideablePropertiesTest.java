package com.vmturbo.components.test.utilities;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

//@RunWith(PowerMockRunner.class)
//// We need to mock System.class, so follow the approach outlined:
////   https://raw.githubusercontent.com/wiki/powermock/powermock/MockSystem.md
//@PrepareForTest({ EnvOverrideableProperties.class })
//// Prevent linkage error:
//// http://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class
//@PowerMockIgnore("javax.management.*")
@Ignore("This unit test fails on Java11. Need to be fixed later")
public class EnvOverrideablePropertiesTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetProperty() {
        final EnvOverrideableProperties props = EnvOverrideableProperties.newBuilder()
            .addProperty("test", "val")
            .build();

        Assert.assertEquals("val", props.get("test"));
    }

    @Test
    public void testInvalidPropertyException() {
        final EnvOverrideableProperties props = EnvOverrideableProperties.newBuilder()
                .addProperty("test", "val")
                .build();

        expectedException.expect(IllegalArgumentException.class);

        props.get("invalid");
    }

    @Test
    public void testSysenvOverride() {
        PowerMockito.mockStatic(System.class);
        final EnvOverrideableProperties props = EnvOverrideableProperties.newBuilder()
                .addProperty("test", "val")
                .build();
        Assert.assertEquals("val", props.get("test"));
        Mockito.when(System.getenv(Mockito.eq("test"))).thenReturn("system");
        Assert.assertEquals("system", props.get("test"));
    }
}
