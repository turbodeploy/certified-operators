package com.vmturbo.integrations.intersight.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.LicensingMoles.LicenseManagerServiceMole;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for the Intersight license sync service.
 */
public class IntersightLicenseSyncServiceTest {

    private LicenseManagerServiceMole licenseManagerService = spy(LicenseManagerServiceMole.class);

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(licenseManagerService);

    private LicenseManagerServiceBlockingStub licenseManagerClient;

    private IntersightLicenseClient intersightLicenseClient = mock(IntersightLicenseClient.class);

    private IntersightLicenseSyncService syncService = new IntersightLicenseSyncService(true, intersightLicenseClient, 0, 0, 0, licenseManagerClient);

    /**
     * set up the license manager client.
     */
    @Before
    public void setup() {
        licenseManagerClient = LicenseManagerServiceGrpc.newBlockingStub(testServer.getChannel());
    }

    /**
     * Verify that no edits are generated when no licenses are found.
     */
    @Test
    public void testDiscoverLicenseEditsNoLicenses() {

        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = syncService.discoverLicenseEdits(
                Collections.emptyList(), Collections.emptyList()
        );

        assertTrue(edits.first.isEmpty());
        assertTrue(edits.second.isEmpty());
    }

    /**
     * Verify that the first intersight license found would be added.
     */
    @Test
    public void testDiscoverLicenseEditsFirstLicense() {
        LicenseLicenseInfo firstLicense = mock(LicenseLicenseInfo.class);
        when(firstLicense.getMoid()).thenReturn("1");
        when(firstLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(firstLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);

        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = syncService.discoverLicenseEdits(
                Collections.emptyList(), ImmutableList.of(firstLicense));

        assertEquals(1, edits.first.size());
        assertTrue(edits.second.isEmpty());
    }

    /**
     * Verify that a local proxy license no longer discovered would be removed.
     */
    @Test
    public void testDiscoverLicenseEditsSingleLicenseRemoved() {
        LicenseDTO onlyLicense = LicenseDTO.newBuilder()
                .setExternalLicenseKey("1")
                .setEdition(IntersightLicenseEdition.IWO_ESSENTIALS.name())
                .build();

        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = syncService.discoverLicenseEdits(
                ImmutableList.of(onlyLicense), Collections.emptyList());

        assertTrue(edits.first.isEmpty());
        assertEquals(1, edits.second.size());
    }

    /**
     * Verify that a locally-installed non-IWO license should NOT be removed even if there is no match for
     * it in the intersight API request.
     */
    @Test
    public void testDiscoverLicenseEditsLeaveNonIWOLicenseAlone() {
        LicenseDTO iwoLicense = LicenseDTO.newBuilder()
                .setUuid("1")
                .setExternalLicenseKey("1")
                .setEdition(IntersightLicenseEdition.IWO_ESSENTIALS.name())
                .build();

        LicenseDTO cwomLicense = LicenseDTO.newBuilder()
                .setUuid("2")
                .setEdition("Advanced")
                .build();

        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = syncService.discoverLicenseEdits(
                ImmutableList.of(cwomLicense, iwoLicense), Collections.emptyList());

        assertTrue(edits.first.isEmpty());
        List<LicenseDTO> licensesToRemove = edits.second;
        assertEquals(1, licensesToRemove.size());

        assertNotEquals("2", licensesToRemove.get(0).getUuid());
    }

    /**
     * If an existing license is updated, we should see that as an add followed by a remove.
     */
    @Test
    public void testDiscoverLicenseEditsUpdateExpired() {
        // we'll simulate an expired license
        LicenseDTO existingLicense = LicenseDTO.newBuilder()
                .setUuid("1")
                .setExternalLicenseKey("1")
                .setEdition(IntersightLicenseEdition.IWO_ESSENTIALS.name())
                .setExpirationDate(ILicense.PERM_LIC)
                .build();

        // the license info with the same moid is now out of compliance
        LicenseLicenseInfo updatedLicense = mock(LicenseLicenseInfo.class);
        when(updatedLicense.getMoid()).thenReturn("1");
        when(updatedLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(updatedLicense.getLicenseState()).thenReturn(LicenseStateEnum.OUTOFCOMPLIANCE);

        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = syncService.discoverLicenseEdits(
                ImmutableList.of(existingLicense), ImmutableList.of(updatedLicense));

        // we should see one add and one delete
        assertEquals(1, edits.first.size());
        assertEquals(1, edits.second.size());

    }
}
