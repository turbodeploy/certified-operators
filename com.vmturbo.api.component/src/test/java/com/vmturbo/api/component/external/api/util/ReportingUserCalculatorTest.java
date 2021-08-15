package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.dto.user.RoleApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.serviceinterfaces.IUsersService.LoggedInUserInfo;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.protobuf.LicenseProtoUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Unit tests for {@link ReportingUserCalculator}.
 */
public class ReportingUserCalculatorTest {

    private final String editorUser = "turbo-report-editor";
    private final int reportEditorCount = 5;

    private LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
    private LicenseSummary licenseSummary = LicenseSummary.newBuilder()
            .setMaxReportEditorsCount(reportEditorCount)
            .build();

    // Use this to push license updates to the client.
    private FluxSink<LicenseSummary> publisher;
    private Flux<LicenseSummary> updateStream;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        updateStream = Flux.create(emitter -> publisher = emitter);
        updateStream.publish().subscribe();
        when(licenseCheckClient.getUpdateEventStream()).thenReturn(updateStream);
        when(licenseCheckClient.geCurrentLicenseSummary()).thenReturn(licenseSummary);
    }

    /**
     * Test that no headers get set in the response when reporting is disabled.
     */
    @Test
    public void testDisabledReports() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(false, "foo", licenseCheckClient);
        LoggedInUserInfo info = calculator.getMe(makeUser("administrator", SecurityConstant.REPORT_EDITOR));
        assertFalse(info.getReportingUserName().isPresent());
    }

    /**
     * Test that for administrators the reporting user header gets set to the report editor.
     */
    @Test
    public void testEnabledReportsEditorRole() {
        final String username = "foo";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        //set an unscoped user in the context
        setUnscopedUserInContext("unscopedUser", ImmutableList.of("ADMINISTRATOR"));
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.AUTOMATOR, SecurityConstant.REPORT_EDITOR));
        assertThat("turbo-report-editor-0", is(info.getReportingUserName().get()));
    }

    /**
     * Test that for non-administrators the reporting user header gets set to the username.
     */
    @Test
    public void testEnabledReportsNonEditorRole() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        //set an unscoped user in the context
        setUnscopedUserInContext("unscopedUser", ImmutableList.of("ADMINISTRATOR"));
        LoggedInUserInfo info = calculator.getMe(makeUser(editorUser, SecurityConstant.ADMINISTRATOR));
        assertThat(editorUser, is(info.getReportingUserName().get()));
    }

    /**
     * Tests that for a scoped user, the reportingUserName is set to null (the user will not have access to reports).
     *
     * <p>Reports access is now evaluated on reporting being evaluated and user being unscoped.
     */
    @Test
    public void testEnabledReportsEditorRoleForScopedUser() {
        final String username = "foo";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        //set a scoped user in the context
        setScopedUserInContext("scopedUser", ImmutableList.of("SHARED_ADVOSOR"));
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.SHARED_ADVISOR, SecurityConstant.SHARED_ADVISOR));
        assert (info.getUserApiDTO() != null);
        assertFalse(info.getReportingUserName().isPresent());
    }

    /**
     * Tests that for a unscoped user, the reportingUserName is not null (the user will have access to reports).
     *
     * <p>Reports access is now evaluated on reporting being evaluated and user being unscoped.
     */
    @Test
    public void testEnabledReportsEditorRoleForUnscopedUser() {
        final String username = "foo";
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        //set an unscoped user in the context
        setUnscopedUserInContext("unscopedUser", ImmutableList.of("ADMINISTRATOR"));
        LoggedInUserInfo info = calculator.getMe(makeUser(username, SecurityConstant.ADMINISTRATOR, SecurityConstant.SHARED_ADVISOR));
        assert (info.getUserApiDTO() != null);
        assertTrue(info.getReportingUserName().isPresent());
    }

    /**
     * Test that resizing a namepool increases in size (downsizing not supported).
     */
    @Test
    public void testIncreaseReportEditorsOnBiggerLicenseSummary() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        for (int i = 0; i < reportEditorCount; i++) {
            assertThat(calculator.getMe(makeUser("user" + i, LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR)).getReportingUserName().get(),
                is(editorUser + "-" + i));
        }
        publisher.next(LicenseSummary.newBuilder()
            .setMaxReportEditorsCount(reportEditorCount + 1)
            .build());
        assertThat(calculator.getMe(makeUser("extraUser", LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR)).getReportingUserName().get(),
                is(editorUser + "-" + reportEditorCount));
    }

    /**
     * Test that when requesting a name on a pool with no more available names.
     */
    @Test
    public void testNamePoolUnavailableName() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        for (int i = 0; i < reportEditorCount; i++) {
            assertThat(calculator.getMe(makeUser("user" + i, LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR)).getReportingUserName().get(),
                    is(editorUser + "-" + i));
        }
        assertThat(calculator.getMe(makeUser("extraUser", LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR)).getReportingUserName(),
                // Not "editorUser"
                is(Optional.of("extraUser")));
    }

    /**
     * Test that delete user with a name is the name pool frees up a name.
     */
    @Test
    public void testUserDeletedRelease() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        UserApiDTO firstUser = makeUser("firstUser", LoginProviderMapper.toApi(PROVIDER.LDAP).toLowerCase(), SecurityConstant.REPORT_EDITOR);
        //set an unscoped users in the context
        setUnscopedUserInContext("firstUser", ImmutableList.of("ADMINISTRATOR"));
        assertThat(calculator.getMe(firstUser).getReportingUserName().get(), is(
                LicenseProtoUtil.formatReportEditorUsername(editorUser, 0)));
        calculator.onUserDeleted(firstUser);
        // The "onUserDeleted" should have freed up the first "editorUser" name for use by the
        // second user.
        UserApiDTO secondUser = makeUser("secondUser", LoginProviderMapper.toApi(PROVIDER.LDAP), SecurityConstant.REPORT_EDITOR);
        //set an unscoped user in the context
        setUnscopedUserInContext("secondUser", ImmutableList.of("ADMINISTRATOR"));
        assertThat(calculator.getMe(secondUser).getReportingUserName().get(), is(
                LicenseProtoUtil.formatReportEditorUsername(editorUser, 0)));
    }

    /**
     * Test that modifying user with a name is the name pool frees up a name.
     */
    @Test
    public void testUserChangedRelease() {
        ReportingUserCalculator calculator = new ReportingUserCalculator(true, editorUser, licenseCheckClient);
        UserApiDTO firstUser = makeUser("firstUser", LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR);
        UserApiDTO secondUser = makeUser("secondUser", LoginProviderMapper.toApi(PROVIDER.LOCAL), SecurityConstant.REPORT_EDITOR);
        assertThat(calculator.getMe(firstUser).getReportingUserName().get(), is(
                LicenseProtoUtil.formatReportEditorUsername(editorUser, 0)));
        // simulate UI sending upper case provider
        UserApiDTO firstUseUpdated = makeUser("firstUser", "LOCAL");
        calculator.onUserModified(firstUseUpdated);
        // The "onUserChanged" should have freed up the first "editorUser" name for use by the
        // second user.
        assertThat(calculator.getMe(secondUser).getReportingUserName().get(), is(
                LicenseProtoUtil.formatReportEditorUsername(editorUser, 0)));
    }

    private UserApiDTO makeUser(String name, String provider, String... roles) {
        UserApiDTO user = new UserApiDTO();
        user.setLoginProvider(provider);
        user.setUuid(name);
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

    /**
     * Helper method to set an unscoped user in the Context.
     *
     * @param userName the user's name.
     * @param roles The list of roles for the user.
     */
    private void setUnscopedUserInContext(String userName, List<String> roles) {
        SecurityContextHolder.getContext().setAuthentication(
                                 new UsernamePasswordAuthenticationToken(
                                         new AuthUserDTO(null,
                                                 userName,
                                                 "password",
                                                 "10.10.10.10",
                                                 "11111",
                                                 "token",
                                                 roles,
                                                 null),
                                         "",
                                         Collections.emptySet()));
    }

    /**
     * Helper method to set an scoped user in the Context.
     *
     * @param userName the user's name.
     * @param roles The list of roles for the user.
     */
    private void setScopedUserInContext(String userName, List<String> roles) {
        SecurityContextHolder.getContext().setAuthentication(
                                         new UsernamePasswordAuthenticationToken(
                                                 new AuthUserDTO(null,
                                                         userName,
                                                         "password",
                                         "10.10.10.10",
                                         "11111",
                                         "token",
                                         roles,
                                         Arrays.asList(1L)),
                                 "",
                                 Collections.emptySet()));
    }
}