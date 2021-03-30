package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.api.dto.user.RoleApiDTO;
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
        LoggedInUserInfo info = calculator.getMe(makeUser("administrator", SecurityConstant.REPORT_EDITOR));
        assertFalse(info.getReportingUserName().isPresent());
    }

    /**
     * Test that for administrators the reporting user header gets set to the report editor.
     */
    @Test
    public void testEnabledReportsEditorRole() {
        final String username = "blah";
        final String editorUser = "iamaneditor";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser);
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.AUTOMATOR, SecurityConstant.REPORT_EDITOR));
        assertThat(info.getReportingUserName().get(), is(editorUser));
    }

    /**
     * Test that for non-administrators the reporting user header gets set to the username.
     */
    @Test
    public void testEnabledReportsNonEditorRole() {
        final String username = "oiarnsto";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, "foo");
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.ADMINISTRATOR));
        assertThat(info.getReportingUserName().get(), is(username));
    }

    private UserApiDTO makeUser(String name, String... roles) {
        UserApiDTO user = new UserApiDTO();
        user.setUsername(name);
        user.setRoles(Stream.of(roles)
            .map(role -> {
                RoleApiDTO dto = new RoleApiDTO();
                dto.setName(role);
                return dto;
            })
            .collect(Collectors.toList()));
        return user;
    }
}