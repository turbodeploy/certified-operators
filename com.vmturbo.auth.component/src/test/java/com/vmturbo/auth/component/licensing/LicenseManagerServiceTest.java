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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.auth.component.store.LicenseLocalStoreTest;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseDeserializer;
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
        LicenseDTO workloadLicense = LicenseDTOUtils.iLicenseToLicenseDTO(
                LicenseDeserializer.deserialize(LicenseLocalStoreTest.WORKLOAD_LICENSE, "test.file"));

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        assertEquals("File name is correct.", "test.file",
                addResponse.getLicenseDTO(0).getFilename());
        assertEquals("No errors saving this most valid of licenses.", 0,
                addResponse.getLicenseDTO(0).getErrorReasonCount());

        // can we get it back?
        GetLicensesResponse workloadLicenses = clientStub.getLicenses(Empty.getDefaultInstance());
        assertEquals("1 license in response", 1, workloadLicenses.getLicenseDTOCount());
        // a hash match would be a good check
        assertEquals(workloadLicense.getLicenseKey(), workloadLicenses.getLicenseDTO(0).getLicenseKey());
    }

    // test bad workload licenses
    @Test
    public void testTamperedLicense() throws IOException {

        // this license has been tampered with and the checksum should fail
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/tampered-license.xml");
        LicenseDTO returnedLicense = addResponse.getLicenseDTO(0);
        assertEquals("This license has an issue.",1, returnedLicense.getErrorReasonCount());
        assertFalse("This license is not so valid.", returnedLicense.getIsValid());
        assertEquals(ErrorReason.INVALID_LICENSE_KEY.name(), returnedLicense.getErrorReason(0));

        // test an expired license
        addResponse = addLicenseFromFile("licenses/expired-license.xml");
        assertEquals(ErrorReason.EXPIRED.name(), addResponse.getLicenseDTO(0).getErrorReason(0));
    }

    @Test
    public void testExpiredLicense() throws IOException {
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/expired-license.xml");
        assertEquals(ErrorReason.EXPIRED.name(), addResponse.getLicenseDTO(0).getErrorReason(0));
    }

    @Test
    public void testSocketLicense() throws IOException {
        // socket licenses are incompatible
        AddLicensesResponse addResponse = addLicenseFromFile("licenses/socket-license.xml");
        assertEquals(ErrorReason.INCOMPATIBLE.name(), addResponse.getLicenseDTO(0).getErrorReason(0));
    }

    /**
     * Convenience method to add a license file from the local file system.
     *
     * @param filename the filename of the license to add
     * @return The response from the server
     * @throws IOException If any probs interacting with the license file
     */
    private AddLicensesResponse addLicenseFromFile(String filename) throws IOException {
        File file = new File(getClass().getClassLoader().getResource(filename).getFile());
        if (! file.exists()) {
            throw new FileNotFoundException();
        }
        InputStream is = new FileInputStream(file);

        LicenseDTO licenseDTO = LicenseDTOUtils.iLicenseToLicenseDTO(
                LicenseDeserializer.deserialize(is, filename));

        return clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(licenseDTO)
                .build());
    }

    @Test
    public void testStoreAndRetrieveCWOMLicense() throws IOException {
        LicenseDTO workloadLicense = LicenseDTOUtils.iLicenseToLicenseDTO(
                LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_LICENSE, "test.file"));

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        assertEquals("File name is correct.", "test.file",
                addResponse.getLicenseDTO(0).getFilename());
        assertEquals("No errors saving this most valid of licenses.", 0,
                addResponse.getLicenseDTO(0).getErrorReasonCount());

        // can we get it back?
        GetLicensesResponse workloadLicenses = clientStub.getLicenses(Empty.getDefaultInstance());
        assertEquals("1 license in response", 1, workloadLicenses.getLicenseDTOCount());
        // a hash match would be a good check
        assertEquals(workloadLicense.getLicenseKey(),
                workloadLicenses.getLicenseDTO(0).getLicenseKey());
    }

    // test a bad CWOM license
    @Test
    public void testCWOMBad() throws IOException {
        LicenseDTO workloadLicense = LicenseDTOUtils.iLicenseToLicenseDTO(
                LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_INVALID_LICENSE, "test.file"));

        AddLicensesResponse addResponse = clientStub.addLicenses(AddLicensesRequest.newBuilder()
                .addLicenseDTO(workloadLicense)
                .build());
        LicenseDTO returnedLicense = addResponse.getLicenseDTO(0);
        // the license should have two issues -- the date and feature sets are invalid.
        assertEquals("This license has issuez.",2,returnedLicense.getErrorReasonCount());
        assertFalse("This license is not so valid.", returnedLicense.getIsValid());
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
        boolean allSaved = response.getLicenseDTOList().stream().allMatch(LicenseDTO::getIsValid);
        Assert.assertTrue(allSaved);

        // create a license summary out of all the licenses we have now
        License combinedLicense = LicenseDTOUtils.combineLicenses(licenseManagerService.getLicenses());
        LicenseSummary licenseSummary = LicenseDTOUtils.createLicenseSummary(combinedLicense, false);
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
        ILicense cwomLicense = LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_LICENSE, "test.file");

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
     * Multiple licenses that have conflicting feature sets should be rejected.
     *
     * @throws IOException if there's a problem reading persisted licenses
     */
    @Test
    public void testValidateMultipleLicensesMixedFeatures() throws IOException {
        String email = "somebody@mail.com";
        List<String> features = Arrays.asList("Feature");
        // put one license in the license store
        Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        ILicense originalLicense = LicenseTestUtils.createLicense(Date.from(tomorrow), email, features, 1);
        licenseKVStore.storeLicense(LicenseDTOUtils.iLicenseToLicenseDTO(originalLicense));

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

    // TODO: When we support authorization in this service (OM-35910), add test validating Admin role
    // requirement for adding / removing licenses.

    // TODO: When we add authorization (OM-35910), add test validating that licenses can still be
    // retrieved without admin role.
}
