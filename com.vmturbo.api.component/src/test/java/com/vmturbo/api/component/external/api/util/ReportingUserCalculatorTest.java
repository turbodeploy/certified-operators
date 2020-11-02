package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.serviceinterfaces.IUsersService.LoggedInUserInfo;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;

/**
 * Unit tests for {@link ReportingUserCalculator}.
 */
public class ReportingUserCalculatorTest {

    /**
     * Test that no headers get set in the response when reporting is disabled.
     */
    @Test
    public void testDisabledReports() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(false, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser("admin", SecurityConstant.ADMINISTRATOR));
        assertFalse(info.getReportingUserName().isPresent());
    }

    /**
     * Test that for administrators the reporting user header gets set to the username.
     */
    @Test
    public void testEnabledReportsAdministrator() {
        final String username = "oiarnsto";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.ADMINISTRATOR));
        assertThat(info.getReportingUserName().get(), is(username));
    }

    /**
     * Test that for non-administrators the reporting user header gets set to the viewer user.
     */
    @Test
    public void testEnabledReportsNonAdministrator() {
        final String username = "oiarnsto";
        final String viewerUser = "iamaviewer";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, viewerUser);
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.OBSERVER));
        assertThat(info.getReportingUserName().get(), is(viewerUser));
    }

    private UserApiDTO makeUser(String name, String role) {
        UserApiDTO user = new UserApiDTO();
        user.setUsername(name);
        user.setRoleName(role);
        return user;
    }
}