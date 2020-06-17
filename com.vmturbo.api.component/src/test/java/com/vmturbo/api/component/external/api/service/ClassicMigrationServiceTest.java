package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.AccountField;

/**
 * Test service for fetching workflows.
 */
@RunWith(MockitoJUnitRunner.class)
public class ClassicMigrationServiceTest {

    // The class under test
    private ClassicMigrationService classicMigrationService;

    private final TargetsService targetsService = mock(TargetsService.class);

    /**
     * Expect no exceptions, by default.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Initial test setup.
     *
     * <p>Set up the GRPC test server to channel messages to the workflowServiceMole.
     * Instantiate the {@link WorkflowsService}, the class under test.</p>
     */
    @Before
    public void setup() {
        classicMigrationService = new ClassicMigrationService(targetsService);
    }

    /**
     * Tests classic target addition. This test makes sure that the password is decrypted
     * correctly using the decryption key
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddClassicTarget() throws Exception {
        final long probeId = 1;
        final String secretFieldName1 = "encryptedPassword";
        final String secretFieldName2 = "secretApiKey";
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category",
            createAccountDef("key"),  createAccountDef("password"));
        final String encryptedPassword = "AAAAAQAAACAZmSKjV/u/aiaMiZqNOViVh+pvJ0yo4+gOqb5cRQ1qHwAAABBrv8Vz7wx5HYlTrb7rn1LTAAAAEEONJRXcXRk7Ba4hQ/9L9HY=";
        final String encryptionKey = "$_1_$VMT$$925k0bdHby8owGqtOqhv2hZNHDz1DWdMv3TFskyBT7in6DwlqPO/nO/BNDfg99u2YC0=";
        final String decryptedKey = "?etMhAJ2RDs*SHJ";
        Collection<InputFieldApiDTO> inputFields = Arrays.asList(inputField("key", "value"),
            secretInputField(secretFieldName1, encryptedPassword),
            secretInputField(secretFieldName2, encryptedPassword),
            inputField(StringConstants.ENCRYPTION_KEY,
                encryptionKey));
        classicMigrationService.migrateClassicTarget(probe.getType(),
            inputFields);
        final ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(targetsService).createTarget(Mockito.eq(probe.getType()), captor.capture());
        Collection<InputFieldApiDTO> capturedInputFields = captor.getValue();
        Assert.assertEquals(decryptedKey,
            capturedInputFields.stream()
                .filter(av -> av.getName().equals(secretFieldName1)).findAny().get().getValue());
        Assert.assertEquals(decryptedKey,
            capturedInputFields.stream()
                .filter(av -> av.getName().equals(secretFieldName2)).findAny().get().getValue());
        Assert.assertFalse(capturedInputFields.stream()
            .anyMatch(av -> av.getName().equals(StringConstants.ENCRYPTION_KEY) || av.getName().equals(
                StringConstants.ENCRYPTION_KEY)));
    }

    /**
     * Decrypt a cipher text that was encrypted in a classic instance, using the the encryption key.
     *
     * @throws Exception if failing to decrypt.
     */
    @Test
    public void testDecryptClassicTarget() throws Exception {
        String ciphertext = "AAAAAQAAACBIqf/13eLIzdroXvYgtxCe3LeKNn08LmK2tvR4iY8/KgAAABBe4p0Zrwxbveag2FKQoK8CAAAAIItOJIqyygSajyjo6irA0lgkcjhek6GFAHB9Y9Wkwfkl";
        String key = "$_1_$VMT$$J0D6a+P+J4j4PBY88Qjq87Zl/JVVKmCg6TQGuaULkrmTuhPGREh1Rvt4z39PAG/Sh"
            + "+1tIN566fYuvBiVfrR76mQf3SP3dEfv2PTp6TFSddo1e6AlsyyO1KV06k4T+yc4ln1guXSgr2OEPinzT8VyTERVxECs/2GZuJbDS4QQ+2ajEPPIQOH4el/Yt7ax";
        Assert.assertEquals("Sysdreamworks123", classicMigrationService.decryptClassicCiphertext(ciphertext, key));
    }

    private InputFieldApiDTO secretInputField(String key, String value) {
        final InputFieldApiDTO result = new InputFieldApiDTO();
        result.setName(key);
        result.setValue(value);
        result.setIsSecret(true);
        return result;
    }

    private InputFieldApiDTO inputField(String key, String value) {
        final InputFieldApiDTO result = new InputFieldApiDTO();
        result.setName(key);
        result.setValue(value);
        return result;
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category,
                                          AccountDefEntry... entries) throws Exception {
        return createMockProbeInfo(probeId, type, category, CreationMode.STAND_ALONE, entries);
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category,
                                          CreationMode creationMode, AccountDefEntry... entries) throws Exception {
        final ProbeInfo newProbeInfo = Mockito.mock(ProbeInfo.class);
        when(newProbeInfo.getId()).thenReturn(probeId);
        when(newProbeInfo.getType()).thenReturn(type);
        when(newProbeInfo.getCategory()).thenReturn(category);
        when(newProbeInfo.getAccountDefinitions()).thenReturn(Arrays.asList(entries));
        when(newProbeInfo.getCreationMode()).thenReturn(creationMode);
        if (entries.length > 0) {
            when(newProbeInfo.getIdentifyingFields())
                .thenReturn(Collections.singletonList(entries[0].getName()));
        } else {
            when(newProbeInfo.getIdentifyingFields()).thenReturn(Collections.emptyList());
        }
        return newProbeInfo;
    }


    private static AccountDefEntry createAccountDef(String key) {
        return createAccountDef(key, AccountFieldValueType.STRING);
    }

    private static AccountDefEntry createAccountDef(String key, AccountFieldValueType valueType) {
        return new AccountField(key, key + "-name", key + "-description", true, false, valueType,
            null, Collections.emptyList(), ".*", null);
    }
}
