package com.vmturbo.clustermgr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.clustermgr.DiagEnvironmentSummary.ChannelFactory;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.licensing.LicensingMoles.LicenseManagerServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.BuildProperties;

/**
 * Unit tests for {@link DiagEnvironmentSummary}.
 *
 */
public class DiagEnvironmentSummaryTest {

    private static final String VERSION = "1.2";

    private static final String SHORT_COMMIT = "ositaenrs";

    private LicenseManagerServiceMole licenseBackend = spy(LicenseManagerServiceMole.class);

    private DiagEnvironmentSummary diagEnvironmentSummary;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private BuildProperties buildProperties = mock(BuildProperties.class);

    /**
     * gRPC server to mock out inter-component dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(licenseBackend);

    private ChannelFactory channelFactory = mock(ChannelFactory.class);

    /**
     * Common setup code for all tests.
     */
    @Before
    public void setup() {
        when(channelFactory.getChannel("auth", 192)).thenReturn(grpcTestServer.getChannel());
        diagEnvironmentSummary = new DiagEnvironmentSummary(
            buildProperties,
            clock,
            channelFactory, "auth", 192);
        when(buildProperties.getVersion()).thenReturn(VERSION);
        when(buildProperties.getShortCommitId()).thenReturn(SHORT_COMMIT);
    }

    /**
     * Test getting the diag file name when there is no license information in the auth component.
     */
    @Test
    public void testNoLicense() {
        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        assertThat(diagFileName, containsString(VERSION));
        assertThat(diagFileName, containsString(SHORT_COMMIT));
        assertThat(diagFileName, containsString(LocalDateTime.now(clock).toLocalDate().toString()));
        assertThat(diagFileName, containsString("_" + clock.millis()));
    }

    /**
     * Test getting the diag file name when there is no email in the license information
     * in the auth component.
     */
    @Test
    public void testLicenseNoEmail() {
        doReturn(GetLicensesResponse.newBuilder()
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEdition("foo")))
            .build()).when(licenseBackend).getLicenses(GetLicensesRequest.getDefaultInstance());

        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        assertThat(diagFileName, containsString(VERSION));
        assertThat(diagFileName, containsString(SHORT_COMMIT));
        assertThat(diagFileName, containsString(LocalDateTime.now(clock).toLocalDate().toString()));
        assertThat(diagFileName, containsString("_" + clock.millis()));
    }

    /**
     * Test getting the diag file name when there is no domain name in the email in the
     * license information in the auth component.
     */
    @Test
    public void testLicenseEmailNoDomain() {
        doReturn(GetLicensesResponse.newBuilder()
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEmail("myemail")
                    .setEdition("foo")))
            .build()).when(licenseBackend).getLicenses(GetLicensesRequest.getDefaultInstance());

        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        assertThat(diagFileName, containsString(VERSION));
        assertThat(diagFileName, containsString(SHORT_COMMIT));
        assertThat(diagFileName, containsString(LocalDateTime.now(clock).toLocalDate().toString()));
        assertThat(diagFileName, containsString("_" + clock.millis()));
    }

    /**
     * Test getting the diag file name with the domain name extracted from the license.
     */
    @Test
    public void testDisplayNameWithDomain() {
        doReturn(GetLicensesResponse.newBuilder()
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEmail("myemail@turbonomic.com")
                    .setEdition("foo")))
            .build()).when(licenseBackend).getLicenses(GetLicensesRequest.getDefaultInstance());

        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        assertThat(diagFileName, containsString(VERSION));
        assertThat(diagFileName, containsString(SHORT_COMMIT));
        assertThat(diagFileName, containsString("turbonomic.com"));
        assertThat(diagFileName, containsString("_" + clock.millis()));
    }

    /**
     * Test with the email in the license having a trailing white space.
     * This might trigger the filename to have a white space, and might fail the upload to the
     * server, because right now it's not supporting it.
     */
    @Test
    public void testLicenseEmailTrailingSpace() {
        doReturn(GetLicensesResponse.newBuilder()
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEmail("myemail@turbonomic.io ")
                    .setEdition("foo")))
            .build()).when(licenseBackend).getLicenses(GetLicensesRequest.getDefaultInstance());

        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        // check that the filename should not containg any whitespaces, otherwise the upload to the
        // server will fail
        assertThat(diagFileName.indexOf(" "), is(-1));
    }

    /**
     * Test getting the diag file name with the multiple domain names extracted from the license.
     */
    @Test
    public void testDisplayNameWithMultipleDomains() {
        doReturn(GetLicensesResponse.newBuilder()
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEmail("myemail@turbonomic.com")))
            .addLicenseDTO(LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                    .setEmail("myemail@vmturbo.com")))
            .build()).when(licenseBackend).getLicenses(GetLicensesRequest.getDefaultInstance());

        final String diagFileName = diagEnvironmentSummary.getDiagFileName();
        assertThat(diagFileName, containsString(VERSION));
        assertThat(diagFileName, containsString(SHORT_COMMIT));
        assertThat(diagFileName, containsString("turbonomic.com_vmturbo.com"));
        assertThat(diagFileName, containsString(LocalDateTime.now(clock).toLocalDate().toString()));
        assertThat(diagFileName, containsString("_" + clock.millis()));
    }

}