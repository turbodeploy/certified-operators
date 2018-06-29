package com.vmturbo.auth.component.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Empty;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.auth.component.store.LicenseLocalStoreTest;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.licensing.utils.LicenseDeserializer;

/**
 * LicenseManagerService tests
 */
public class LicenseManagerServiceTest {

    // set up a license manager service backed by a map.
    MapKeyValueStore mapStore = new MapKeyValueStore();
    LicenseKVStore licenseKVStore = new LicenseKVStore(mapStore);
    LicenseManagerService licenseManagerService = new LicenseManagerService(licenseKVStore);

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

    // TODO: When we support authorization in this service (OM-35910), add test validating Admin role
    // requirement for adding / removing licenses.

    // TODO: When we add authorization (OM-35910), add test validating that licenses can still be
    // retrieved without admin role.
}
