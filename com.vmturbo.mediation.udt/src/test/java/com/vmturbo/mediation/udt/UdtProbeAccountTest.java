package com.vmturbo.mediation.udt;

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Test class for {@link UdtProbeAccount}.
 */
public class UdtProbeAccountTest {

    /**
     * Test account`s target name.
     */
    @Test
    public void testTargetName() {
        UdtProbeAccount account = new UdtProbeAccount();
        Assert.assertEquals("User-defined entities", account.getTargetName());
    }

    /**
     * Tests that 'targetName' field is the target ID.
     *
     * @throws Exception if this field is not found.
     */
    @Test
    public void testTargetId() throws Exception {
        Field field = UdtProbeAccount.class.getDeclaredField("targetName");
        Assert.assertNotNull(field);
        AccountValue accountValueAnnotation = field.getAnnotation(AccountValue.class);
        Assert.assertNotNull(accountValueAnnotation);
        Assert.assertTrue(accountValueAnnotation.targetId());
    }
}
