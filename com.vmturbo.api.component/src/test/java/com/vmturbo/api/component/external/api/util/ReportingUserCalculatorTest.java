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
        ReportingUserCalculator calculator = new ReportingUserCalculator(false, false, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser("administrator"));
        assertFalse(info.getReportingUserName().isPresent());
    }

    /**
     * Test that for administrators the reporting user header gets set to the username.
     */
    @Test
    public void testEnabledReportsAdministrator() {
        final String username = "administrator";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, false, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser(username));
        assertThat(info.getReportingUserName().get(), is(username));
    }

    /**
     * Test that for administrators the reporting user header gets set to the username in a case
     * insensitive fashion.
     */
    @Test
    public void testEnabledReportsAdministratorCaseInsensitive() {
        final String username = "AdministRator";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, false, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser(username));
        assertThat(info.getReportingUserName().get(), is(username));
    }

    /**
     * Test that for non-administrators the reporting user header gets set to the viewer user.
     */
    @Test
    public void testEnabledReportsNonAdministrator() {
        final String username = "oiarnsto";
        final String viewerUser = "iamaviewer";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, false, viewerUser);
        LoggedInUserInfo info = calculator.getMe(makeUser(username));
        assertThat(info.getReportingUserName().get(), is(viewerUser));
    }

    /**
     * Test that the reporting user is set when SAML is enabled and the role is admin.
     */
    @Test
    public void testSamlEnabledAdministratorRole() {
        final String username = "administrator";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, true, "foo");
        UserApiDTO userApiDTO = makeUser(username);
        userApiDTO.setRoleName(SecurityConstant.ADMINISTRATOR);
        LoggedInUserInfo info = calculator.getMe(userApiDTO);
        assertThat(info.getReportingUserName().get(), is(username));
    }

    /**
     * Test that the reporting user is the viewer user when SAML is enabled and the role is non-admin.
     */
    @Test
    public void testSamlEnabledNotAdministratorRole() {
        final String username = "administrator";
        final String viewerUser = "iamaviewer";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, true, viewerUser);
        UserApiDTO userApiDTO = makeUser(username);
        userApiDTO.setRoleName(SecurityConstant.SITE_ADMIN);
        LoggedInUserInfo info = calculator.getMe(userApiDTO);
        assertThat(info.getReportingUserName().get(), is(viewerUser));
    }

    private UserApiDTO makeUser(String name) {
        UserApiDTO user = new UserApiDTO();
        user.setUsername(name);
        return user;
    }
}