package com.vmturbo.auth.component.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.auth.component.store.LicenseLocalStoreTest;
import com.vmturbo.common.protobuf.licensing.Licensing;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseFilter;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesResponse;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseDeserializer;
import com.vmturbo.licensing.utils.LicenseUtil;
import com.vmturbo.notification.api.NotificationSender;

/**
 * LicenseManagerService tests
 */
public class LicenseManagerServiceTest {

    // set up a license manager service backed by a map.
    MapKeyValueStore mapStore = new MapKeyValueStore();
    LicenseKVStore licenseKVStore = new LicenseKVStore(mapStore);
    LicenseManagerService licenseManagerService = new LicenseManagerService(licenseKVStore, mock(
            NotificationSender.class));

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(licenseManagerService);

    private LicenseManagerServiceBlockingStub clientStub;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        // for the encryption key
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());

        IdentityGenerator.initPrefix(0);
        clientStub = LicenseManagerServiceGrpc.newBlockingStub(testServer.getChannel());

        // remove all licenses from the kv store
        mapStore.clear();
    }

    // gary's scenarios:
    // non-admin cannot store a license
    // unauthenticated user can retrieve a license.
    @Test
    public void testStoreAndRetrieveWorkloadLicense() throws IOException {
        // create a workload license DTO by loading it to a LicenseApiDTO, then converting to a
        // LicenseDTO.
        LicenseDTO workloadLicense = LicenseDeserializer.deserialize(LicenseLocalStoreTest.WORKLOAD_LICENSE, "test.file");

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        assertEquals("File name is correct.", "test.file",
                addResponse.getLicenseDTO(0).getFilename());
        assertEquals("No errors saving this most valid of licenses.", 0,
                addResponse.getLicenseDTO(0).getTurbo().getErrorReasonCount());

        // can we get it back?
        GetLicensesResponse workloadLicenses = clientStub.getLicenses(GetLicensesRequest.getDefaultInstance());
        assertEquals("1 license in response", 1, workloadLicenses.getLicenseDTOCount());
        // a hash match would be a good check
        assertEquals(workloadLicense.getTurbo().getLicenseKey(), workloadLicenses.getLicenseDTO(0).getTurbo().getLicenseKey());
    }

    // test bad workload licenses
    @Test
    public void testTamperedLicense() throws IOException {

        // this license has been tampered with and the checksum should fail
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/tampered-license.xml");
        LicenseDTO returnedLicense = addResponse.getLicenseDTO(0);
        assertEquals("This license has an issue.", 1, returnedLicense.getTurbo().getErrorReasonCount());
        assertFalse("This license is not so valid.", returnedLicense.getTurbo().getIsValid());
        assertEquals(ErrorReason.INVALID_LICENSE_KEY.name(), returnedLicense.getTurbo().getErrorReason(0));

        // test an expired license
        addResponse = addLicenseFromFile("licenses/expired-license.xml");
        assertEquals(ErrorReason.EXPIRED.name(), addResponse.getLicenseDTO(0).getTurbo().getErrorReason(0));
    }

    @Test
    public void testExpiredLicense() throws IOException {
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/expired-license.xml");
        assertEquals(ErrorReason.EXPIRED.name(), addResponse.getLicenseDTO(0).getTurbo().getErrorReason(0));
    }

    @Test
    public void testSocketLicense() throws IOException {
        // socket licenses are incompatible
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/socket-license.xml");
        assertEquals(ErrorReason.INCOMPATIBLE.name(), addResponse.getLicenseDTO(0).getTurbo().getErrorReason(0));
    }

    /**
     * Convenience method to add a license file from the local file system.
     *
     * @param filename the filename of the license to add
     * @return The response from the server
     * @throws IOException If any probs interacting with the license file
     */
    private AddLicensesResponse addLicenseFromFile(String filename) throws IOException {
        File file = ResourcePath.getTestResource(LicenseManagerServiceTest.class, filename).toFile();
        if (!file.exists()) {
            throw new FileNotFoundException();
        }
        InputStream is = new FileInputStream(file);
        String str = IOUtils.toString(is, "UTF-8");

        LicenseDTO licenseDTO = LicenseDeserializer.deserialize(str, filename);

        return clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(licenseDTO)
                .build());
    }

    @Test
    public void testStoreAndRetrieveCWOMLicense() throws IOException {
        LicenseDTO workloadLicense = LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_LICENSE, "test.file");

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        assertEquals("File name is correct.", "test.file",
                addResponse.getLicenseDTO(0).getFilename());
        assertEquals("No errors saving this most valid of licenses.", 0,
                addResponse.getLicenseDTO(0).getTurbo().getErrorReasonCount());

        // can we get it back?
        GetLicensesResponse workloadLicenses = clientStub.getLicenses(GetLicensesRequest.getDefaultInstance());
        assertEquals("1 license in response", 1, workloadLicenses.getLicenseDTOCount());
        // a hash match would be a good check
        assertEquals(workloadLicense.getTurbo().getLicenseKey(),
                workloadLicenses.getLicenseDTO(0).getTurbo().getLicenseKey());
    }

    // test a bad CWOM license
    @Test
    public void testCWOMBad() throws IOException {
        LicenseDTO workloadLicense = LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_INVALID_LICENSE, "test.file");

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        LicenseDTO returnedLicense = addResponse.getLicenseDTO(0);
        // the license should have two issues -- the date and feature sets are invalid.
        assertEquals("This license has issuez.", 2, returnedLicense.getTurbo().getErrorReasonCount());
        assertFalse("This license is not so valid.", returnedLicense.getTurbo().getIsValid());
    }

    @Test
    public void testUpgradeExpiredLicense() throws IOException {
        String email = "somebody@mail.com";
        List<String> originalFeatures = Arrays.asList("Feature");
        // put an expired license in the license store
        Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        ILicense expiredLicense = LicenseTestUtils.createLicense(
                Date.from(yesterday), "somebody@mail.com", originalFeatures, 1);
        licenseKVStore.storeLicense(LicenseDTOUtils.iLicenseToLicenseDTO(expiredLicense));

        // verify that you can't upgrade to another expired license
        ILicense anotherExpiredLicense = LicenseTestUtils.createLicense(
                Date.from(yesterday), "somebody@mail.com", originalFeatures, 1);
        Assert.assertFalse(licenseManagerService.validateLicense(anotherExpiredLicense).isValid());

        // verify that adding a non-expired license with the same feature set would be fine
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        ILicense newLicenseSameFeatures = LicenseTestUtils.createLicense(
                Date.from(tomorrow), "somebody@mail.com", originalFeatures, 1);
        Assert.assertTrue(licenseManagerService.validateLicense(newLicenseSameFeatures).isValid());

        // verify that adding a non-expired license with different feature set would be fine too.
        // this would have broken in OM-42491
        ILicense newLicenseNewFeatures = LicenseTestUtils.createLicense(
                Date.from(tomorrow), "somebody@mail.com", Arrays.asList("NewFeature1", "NewFeature2"), 1);
        Assert.assertTrue(licenseManagerService.validateLicense(newLicenseNewFeatures).isValid());

        // store the non-expired license with different features, and verify that the old features
        // are no longer available but the new ones are.
        AddLicensesResponse response = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(LicenseDTOUtils.iLicenseToLicenseDTO(newLicenseNewFeatures))
                .build());
        boolean allSaved = response.getLicenseDTOList().stream()
                .allMatch(l -> l.getTurbo().getIsValid());
        Assert.assertTrue(allSaved);

        // create a license summary out of all the licenses we have now
        LicenseSummary licenseSummary = new LicenseSummaryCreator(1)
            .createLicenseSummary(licenseManagerService.getLicenses(), Optional.empty());
        // verify old features no longer available
        Assert.assertFalse(licenseSummary.getFeatureList().contains("Feature"));
        // verify new features ARE avaialble
        Assert.assertTrue(licenseSummary.getFeatureList().containsAll(Arrays.asList("NewFeature1","NewFeature2")));

    }

    /**
     * Adding multiple licenses that are compatible with an existing license should succeed.
     *
     * @throws IOException if there's a problem reading the file-based licenses.
     */
    @Test
    public void testValidateMultipleLicensesCompatibleWithExisting() throws IOException {
        String email = "somebody@mail.com";
        List<String> features = Arrays.asList("Feature");
        // put one license in the license store
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        ILicense originalLicense = LicenseTestUtils.createLicense(Date.from(tomorrow), email, features, 1);
        licenseKVStore.storeLicense(LicenseDTOUtils.iLicenseToLicenseDTO(originalLicense));

        // now try adding two more licenses that are compatible
        ILicense license1 = LicenseTestUtils.createLicense(Date.from(tomorrow.plus(2, ChronoUnit.DAYS)), email, features, 1);
        ILicense license2 = LicenseTestUtils.createLicense(Date.from(tomorrow.plus(1, ChronoUnit.DAYS)), email, features, 1);

        Collection<ILicense> validatedLicenses = licenseManagerService.validateMultipleLicenses(ImmutableList.of(license1, license2));
        // both licenses should be valid
        Predicate<ILicense> licenseMatcher = ILicense::isValid;
        assertTrue(validatedLicenses.stream().allMatch(licenseMatcher));
    }

    /**
     * Adding two licenses of different editions (cwom and xl) should fail.
     *
     * @throws IOException if there's a problem reading persisted licenses
     */
    @Test
    public void testValidateMultipleLicensesMixedEditions() throws IOException {
        ILicense cwomLicense = LicenseUtil.toModel(LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_LICENSE, "test.file")).get();

        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        ILicense xlLicense = LicenseTestUtils.createLicense(
                Date.from(tomorrow), "somebody@mail.com", Arrays.asList("Feature"), 1);

        Collection<ILicense> validatedLicenses = licenseManagerService.validateMultipleLicenses(ImmutableList.of(cwomLicense, xlLicense));
        // both licenses should be invalid
        Predicate<ILicense> licenseMatcher = license -> {
            // we expect all of the licenses to be marked as invalid w/Invalid Feature Set
            return (!license.isValid() && license.getErrorReasons().contains(ErrorReason.INVALID_FEATURE_SET));
        };
        assertTrue(validatedLicenses.stream().allMatch(licenseMatcher));
    }

    /**
     * Multiple incoming licenses that have conflicting feature sets should be rejected, even if
     * there is no licnese installed yet.
     *
     * @throws IOException if there's a problem reading persisted licenses
     */
    @Test
    public void testValidateMultipleLicensesMixedFeatures() throws IOException {
        String email = "somebody@mail.com";
        // put one license in the license store
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);

        // now try adding two more licenses that have differing feature sets
        ILicense license1 = LicenseTestUtils.createLicense(Date.from(tomorrow.plus(2, ChronoUnit.DAYS)), email,
                Arrays.asList("Feature", "NewFeature"), 1);
        ILicense license2 = LicenseTestUtils.createLicense(Date.from(tomorrow.plus(1, ChronoUnit.DAYS)), email,
                Arrays.asList("Feature", "NewOldFeature"), 1);

        Collection<ILicense> validatedLicenses = licenseManagerService.validateMultipleLicenses(ImmutableList.of(license1, license2));
        // both licenses should be invalid and marked with invalid feature sets.
        Predicate<ILicense> licenseMatcher = license -> (!license.isValid())
                && license.getErrorReasons().contains(ErrorReason.INVALID_FEATURE_SET);
        assertTrue(validatedLicenses.stream().allMatch(licenseMatcher));
    }

    /**
     * Multiple licenses that have conflicting feature sets should be rejected -- even if one license
     * contains a superset of the other's features.
     *
     * @throws IOException if there's a problem reading persisted licenses
     */
    @Test
    public void testValidateMultipleLicensesOverlappingFeatures() throws IOException {
        String email = "somebody@mail.com";
        List<String> features = Arrays.asList("Feature");
        // put one license in the license store
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        ILicense originalLicense = LicenseTestUtils.createLicense(Date.from(tomorrow), email, features, 1);
        licenseKVStore.storeLicense(LicenseDTOUtils.iLicenseToLicenseDTO(originalLicense));

        // now try adding a new license that has a superset of the current license
        ILicense license1 = LicenseTestUtils.createLicense(Date.from(tomorrow.plus(1, ChronoUnit.DAYS)), email,
                Arrays.asList("Feature", "NewOldFeature"), 1);

        Collection<ILicense> validatedLicenses = licenseManagerService.validateMultipleLicenses(ImmutableList.of(license1));
        // both licenses should be invalid and marked with invalid feature sets.
        Predicate<ILicense> licenseMatcher = license -> (!license.isValid())
                && license.getErrorReasons().contains(ErrorReason.INVALID_FEATURE_SET);
        assertTrue(validatedLicenses.stream().allMatch(licenseMatcher));
    }

    /**
     * Adding multiple licenses with the same key should be rejected
     *
     * @throws IOException if there's a problem reading persisted licenses
     */
    @Test
    public void testValidateMultipleLicensesDuplicates() throws IOException {
        String email = "somebody@mail.com";
        List<String> features = Arrays.asList("Feature");
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        // now try adding two more licenses that will be considered duplicates based on the license key
        // same date / features / email / workload == same license key
        ILicense license1 = LicenseTestUtils.createLicense(Date.from(tomorrow), email, features, 1);
        ILicense license2 = LicenseTestUtils.createLicense(Date.from(tomorrow), email, features, 1);

        Collection<ILicense> validatedLicenses = licenseManagerService.validateMultipleLicenses(ImmutableList.of(license1, license2));
        // both licenses should be rejected as dupes
        Predicate<ILicense> licenseMatcher = license -> (!license.isValid())
                && (license.getErrorReasons().contains(ErrorReason.DUPLICATE_LICENSE));
        assertTrue(validatedLicenses.stream().allMatch(licenseMatcher));
    }

    /**
     * Test that allows to add only one Grafana license.
     */
    @Test
    public void testFailedToAddSeveralGrafanaLicenses() {
        // ARRANGE
        final LicenseDTO grafanaLicense1 = LicenseTestUtils.createExternalLicense(Type.GRAFANA,
                LicenseTestUtils.createGrafanaJwtToken(2), LocalDateTime.MAX);
        final LicenseDTO grafanaLicense2 = LicenseTestUtils.createExternalLicense(Type.GRAFANA,
                LicenseTestUtils.createGrafanaJwtToken(5), LocalDateTime.MAX);
        final AddLicensesResponse addLicensesResponse1 = clientStub.addLicenses(
                AddLicensesRequest.newBuilder().addLicenseDTO(grafanaLicense1).buildPartial());

        // check that first grafana license was valid and successfully added
        Assert.assertTrue(
                addLicensesResponse1.getLicenseDTO(0).getExternal().getErrorReasonList().isEmpty());
        final GetLicensesRequest getAllGrafanaLicensesRequest = GetLicensesRequest.newBuilder()
                .setFilter(LicenseFilter.newBuilder().setExternalLicenseType(Type.GRAFANA).build())
                .buildPartial();
        final GetLicensesResponse existedGrafanaLicenses =
                clientStub.getLicenses(getAllGrafanaLicensesRequest);
        Assert.assertEquals(1, existedGrafanaLicenses.getLicenseDTOList().size());

        // ACT - attempt to add another grafana license
        final AddLicensesResponse addLicensesResponse2 = clientStub.addLicenses(
                AddLicensesRequest.newBuilder().addLicenseDTO(grafanaLicense2).buildPartial());

        // ASSERT
        // check that validation of second grafana license was failed (due to restriction to have
        // only one grafana license) and license wasn't added
        Assert.assertFalse(
                addLicensesResponse2.getLicenseDTO(0).getExternal().getErrorReasonList().isEmpty());
        Assert.assertTrue(addLicensesResponse2.getLicenseDTO(0)
                .getExternal()
                .getErrorReasonList()
                .contains(ErrorReason.DUPLICATE_LICENSE.name()));
        final GetLicensesResponse existedGrafanaLicenses2 =
                clientStub.getLicenses(getAllGrafanaLicensesRequest);
        Assert.assertEquals(1, existedGrafanaLicenses2.getLicenseDTOList().size());
    }

    /**
     * Tests successful adding of different external licenses of the same type.
     * payload.
     */
    @Test
    public void testAddingMultipleDifferentExternalLicenses() {
        // ARRANGE
        final LicenseDTO externalLicense1 =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Licence payload 1", null);
        final LicenseDTO externalLicense2 =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Licence payload 2", null);

        // ACT
        final AddLicensesResponse addLicensesResponse = clientStub.addLicenses(
                AddLicensesRequest.newBuilder()
                        .addAllLicenseDTO(Arrays.asList(externalLicense1, externalLicense2))
                        .build());

        // ASSERT
        Assert.assertEquals(2, addLicensesResponse.getLicenseDTOList().size());
        Assert.assertTrue(addLicensesResponse.getLicenseDTOList()
                .stream()
                .allMatch(license -> license.getExternal().getErrorReasonList().isEmpty()));
        Assert.assertEquals(2, clientStub.getLicenses(GetLicensesRequest.getDefaultInstance())
                .getLicenseDTOList()
                .size());
    }

    /**
     * Rejecting to add multiple grafana licenses. All licenses will have {@link
     * ErrorReason#DUPLICATE_LICENSE} failed validation reason.
     */
    @Test
    public void testRejectAddingMultipleGrafanaLicenses() {
        // ARRANGE
        final LicenseDTO grafanaLicense1 = LicenseTestUtils.createExternalLicense(Type.GRAFANA,
                LicenseTestUtils.createGrafanaJwtToken(2), LocalDateTime.MAX);
        final LicenseDTO grafanaLicense2 = LicenseTestUtils.createExternalLicense(Type.GRAFANA,
                LicenseTestUtils.createGrafanaJwtToken(5), LocalDateTime.MAX);

        // ACT
        final AddLicensesResponse addLicensesResponse = clientStub.addLicenses(
                AddLicensesRequest.newBuilder()
                        .addAllLicenseDTO(Arrays.asList(grafanaLicense1, grafanaLicense2))
                        .build());

        // ASSERT
        Assert.assertEquals(2, addLicensesResponse.getLicenseDTOList().size());
        Assert.assertTrue(addLicensesResponse.getLicenseDTOList()
                .stream()
                .allMatch(license -> license.getExternal()
                        .getErrorReasonList()
                        .contains(ErrorReason.DUPLICATE_LICENSE.name())));
        Assert.assertTrue(clientStub.getLicenses(GetLicensesRequest.getDefaultInstance())
                .getLicenseDTOList()
                .isEmpty());
    }

    /**
     * Rejecting to add equals external licenses.
     * Compares payload, type and expiration time of external licenses.
     */
    @Test
    public void testFailedAddingEqualsExternalLicenses() {
        // ARRANGE
        final LicenseDTO externalLicense1 =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Test license payload",
                        LocalDateTime.MAX).toBuilder().setFilename("File 1").build();
        final LicenseDTO externalLicense2 =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Test license payload",
                        LocalDateTime.MAX).toBuilder().setFilename("File 2").build();

        // ACT
        final AddLicensesResponse addLicensesResponse = clientStub.addLicenses(
                AddLicensesRequest.newBuilder()
                        .addAllLicenseDTO(Arrays.asList(externalLicense1, externalLicense2))
                        .build());

        // ASSERT
        Assert.assertEquals(2, addLicensesResponse.getLicenseDTOList().size());
        Assert.assertTrue(addLicensesResponse.getLicenseDTOList()
                .stream()
                .allMatch(license -> license.getExternal()
                        .getErrorReasonList()
                        .contains(ErrorReason.DUPLICATE_LICENSE.name())));
        Assert.assertTrue(clientStub.getLicenses(GetLicensesRequest.getDefaultInstance())
                .getLicenseDTOList()
                .isEmpty());
    }

    /**
     * Test that validation is succeeded for external not expired license.
     */
    @Test
    public void testExternalLicenseSucceededValidation() {
        // ARRANGE
        final LicenseDTO externalLicense =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Test license payload",
                        LocalDateTime.MAX);

        // ACT
        final ValidateLicensesResponse validateLicensesResponse = clientStub.validateLicenses(
                ValidateLicensesRequest.newBuilder().addLicenseDTO(externalLicense).buildPartial());

        // ASSERT
        Assert.assertEquals(1, validateLicensesResponse.getLicenseDTOList().size());
        Assert.assertTrue(validateLicensesResponse.getLicenseDTO(0)
                .getExternal()
                .getErrorReasonList()
                .isEmpty());
    }

    /**
     * Test that validation is failed for external expired license.
     */
    @Test
    public void testExternalLicenseFailedValidation() {
        // ARRANGE
        final LicenseDTO externalLicense =
                LicenseTestUtils.createExternalLicense(Type.UNKNOWN, "Test license payload",
                        LocalDateTime.of(2020, Month.FEBRUARY, 2, 1, 2));

        // ACT
        final ValidateLicensesResponse validateLicensesResponse = clientStub.validateLicenses(
                ValidateLicensesRequest.newBuilder().addLicenseDTO(externalLicense).buildPartial());

        // ASSERT
        Assert.assertEquals(1, validateLicensesResponse.getLicenseDTOList().size());
        final LicenseDTO validatedLicense = validateLicensesResponse.getLicenseDTO(0);
        Assert.assertFalse(validatedLicense.getExternal().getErrorReasonList().isEmpty());
        Assert.assertTrue(validatedLicense.getExternal()
                .getErrorReasonList()
                .contains(ErrorReason.EXPIRED.name()));
    }

    /**
     * Verify that default customer ID is used when not set in the license.
     */
    @Test
    public void testDefaultCustomerId() {
        Licensing.LicenseDTO dto = Licensing.LicenseDTO.newBuilder()
                .setTurbo(Licensing.LicenseDTO.TurboLicense.newBuilder().build())
                .build();

        License license = LicenseDTOUtils.licenseDTOtoLicense(dto).get();
        assertEquals(Licensing.LicenseDTO.TurboLicense.getDefaultInstance().getCustomerId(),
                license.getCustomerId());
    }

    // TODO: When we support authorization in this service (OM-35910), add test validating Admin role
    // requirement for adding / removing licenses.

    // TODO: When we add authorization (OM-35910), add test validating that licenses can still be
    // retrieved without admin role.
}
