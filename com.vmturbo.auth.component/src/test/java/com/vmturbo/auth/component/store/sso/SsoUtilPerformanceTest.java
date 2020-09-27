/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.auth.component.store.sso;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * Performance tests for {@link SsoUtil}.
 */
public class SsoUtilPerformanceTest {
    private final Logger logger = LogManager.getLogger();

    /**
     * Checks performance of simple authorization operation. Expected time to complete this
     * operation is less than 60 seconds.
     */
    @Test
    public void checkAuthorizeSAMLUserInGroup() {
        final SsoUtil ssoUtil = new SsoUtil();
        final Map<String, SecurityGroupDTO> groups = new HashMap<>();
        for (int i = 0; i <= 100_000; i++) {
            final SecurityGroupDTO groupDto = new SecurityGroupDTO(String.format("group#%s", i), "",
                            SecurityConstant.ADMINISTRATOR);
            ssoUtil.putSecurityGroup(groupDto.getDisplayName(), groupDto);
            groups.put(groupDto.getDisplayName(), groupDto);
        }
        final Stopwatch measures = Stopwatch.createStarted();
        for (int i = 0; i <= 100_000; i++) {
            final String groupName = "group#100000";
            final Optional<SecurityGroupDTO> authResult =
                            ssoUtil.authorizeSAMLUserInGroup("alexander", groupName);
            Assert.assertThat(authResult.get(), CoreMatchers.is(groups.get(groupName)));

        }
        final long elapsed = measures.stop().elapsed(TimeUnit.MILLISECONDS);
        logger.info("{} milliseconds", elapsed);
        Assert.assertFalse("Time to authorize user in a group is too high",
                        elapsed > Duration.ofMinutes(1).toMillis());
    }
}
