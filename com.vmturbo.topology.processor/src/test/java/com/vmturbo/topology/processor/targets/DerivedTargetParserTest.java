package com.vmturbo.topology.processor.targets;

import java.util.Arrays;
import java.util.Optional;

import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.OperationManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;

import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Test the {@link DerivedTargetParser}.
 */
public class DerivedTargetParserTest {

    private KeyValueStore keyValueStore;

    private ProbeStore probeStore;

    private TargetStore targetStore;

    private IdentityStore<TargetSpec> identityStore;

    private DerivedTargetParser derivedTargetParser;

    private OperationManager operationManager;

    private long nextTargetId;

    @Before
    public void setup() throws Exception{
        keyValueStore = Mockito.mock(KeyValueStore.class);
        probeStore = Mockito.mock(ProbeStore.class);
        identityStore = new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
        targetStore = new KVBackedTargetStore(keyValueStore, probeStore, identityStore);
        derivedTargetParser = new DerivedTargetParser(probeStore, targetStore);
        operationManager = Mockito.mock(OperationManager.class);
        Mockito.when(operationManager.startValidation(Mockito.anyLong()))
                .thenReturn(Mockito.mock(Validation.class));
        Mockito.when(probeStore.getProbe(probeID1)).thenReturn(Optional.of(probeInfo1));
        Mockito.when(probeStore.getProbeIdForType(probeType1)).thenReturn(Optional.of(probeID1));
        Mockito.when(probeStore.getProbe(probeID2)).thenReturn(Optional.of(probeInfo2));
        Mockito.when(probeStore.getProbeIdForType(probeType2)).thenReturn(Optional.of(probeID2));
        nextTargetId = 10086L;
    }

    private static final String probeCategory = "test";
    // identified by address
    private static final long probeID1 = 1111;
    private static final String probeType1 = "probe1";
    // identified by name
    private static final long probeID2 = 2222;
    private static final String probeType2 = "probe2";

    private static final long parentTargetID1 = 3333;
    private static final long parentTargetID2 = 4444;

    private static final String nameField = "name";
    private static final String addressField = PredefinedAccountDefinition.Address.name().toLowerCase();
    private static final String userNameField = PredefinedAccountDefinition.Username.name().toLowerCase();

    private static final String nameValue = "ABC";
    private static final String usernameValue1 = "vmturbo1";
    private static final String usernameValue2 = "vmturbo2";
    private static final String addrValue1 = "1.2.3.4";
    private static final String addrValue2 = "250.250.250.250";

    final ProbeInfo probeInfo1 = ProbeInfo.newBuilder()
            .setProbeCategory(probeCategory).setProbeType(probeType1)
            .addTargetIdentifierField(addressField)
            .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                            .setName(userNameField)
                            .setDisplayName("username-displayName")
                            .setDescription("username-desc"))
                    .setMandatory(true))
            .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                            .setName(addressField)
                            .setDisplayName("address-displayName")
                            .setDescription("address-desc"))
                    .setMandatory(true))
            .build();

    final ProbeInfo probeInfo2 = ProbeInfo.newBuilder()
            .setProbeCategory(probeCategory).setProbeType(probeType2)
            .addTargetIdentifierField(userNameField)
            .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                            .setName(userNameField)
                            .setDisplayName("username-displayName")
                            .setDescription("username-desc"))
                    .setMandatory(true))
            .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                            .setName(nameField)
                            .setDisplayName("name-displayName")
                            .setDescription("name-desc"))
                    .setMandatory(true)
                    .setIsTargetDisplayName(true))
            .build();

    final DerivedTargetSpecificationDTO dto1 =
            createDerivedTargetSpecDTO(probeType1, true, true, true,
                    userNameField, usernameValue1, addressField, addrValue1);

    final DerivedTargetSpecificationDTO dto2 =
            createDerivedTargetSpecDTO(probeType1, false, false, false,
                    userNameField, usernameValue2, addressField, addrValue2);

    final DerivedTargetSpecificationDTO dto3 =
            createDerivedTargetSpecDTO(probeType1, false, true, true,
                    userNameField, usernameValue1, addressField, addrValue2);

    final DerivedTargetSpecificationDTO dto4 =
            createDerivedTargetSpecDTO(probeType2, false, true, false,
                    userNameField, usernameValue1, nameField, nameValue);

    /**
     * Create a {@link DerivedTargetSpecificationDTO} for test requirements with two associated
     * account values.
     *
     * @param probeType The probe type to be specified.
     * @param isHidden determine whether the derived target is hidden target or not.
     * @param dependent whether the derived target is dependent target or not.
     * @param readOnly whether the derived target is UI readOnly target or not.
     * @param firstKey first AccountValue's key to be added.
     * @param firstValue first AccountValue's value to be added.
     * @param secondKey second AccountValue's key to be added.
     * @param secondValue second AccountValue's value to be added.
     */
    private static final DerivedTargetSpecificationDTO createDerivedTargetSpecDTO(
            String probeType, boolean isHidden,
            boolean dependent, boolean readOnly,
            String firstKey, String firstValue,
            String secondKey, String secondValue) {
        return DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType(probeType).setHidden(isHidden)
                .setDependent(dependent).setReadonly(readOnly)
                .addAccountValue(AccountValue.newBuilder().setKey(firstKey)
                        .setStringValue(firstValue).build())
                .addAccountValue(AccountValue.newBuilder().setKey(secondKey)
                        .setStringValue(secondValue).build())
                .build();
    }

    /**
     * Test instantiate derived targets.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInstantiateDerivedTargetsForAddressField() {
        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto1, dto2));

        Assert.assertEquals(2, targetStore.getAll().size());
        targetStore.getAll().stream().anyMatch(target -> target.getSpec().hasParentId());
        targetStore.getAll().stream().anyMatch(target -> target.getSpec().getIsHidden());
        targetStore.getAll().stream().anyMatch(target -> target.getDisplayName().equals(addrValue1));
        targetStore.getAll().stream().anyMatch(target -> target.getDisplayName().equals(addrValue2));
    }

    /**
     * Test instantiate derived targets.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInstantiateDerivedTargetsForNameField() {
        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID2, Arrays.asList(dto4));

        Assert.assertEquals(1, targetStore.getAll().size());
        targetStore.getAll().stream().allMatch(target -> target.getSpec().hasParentId());
        targetStore.getAll().stream().noneMatch(target -> target.getSpec().getIsHidden());
        targetStore.getAll().stream().allMatch(target -> target.getDisplayName().equals(nameValue));
    }

    /**
     * Tests old derived targets should be removed and new one will be created if parent target update the
     * returned DerivedTargetSpecificationDTO.
     */
    @Test
    public void testUpdateDerivedTargets() {
        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto1, dto2));

        Assert.assertEquals(2, targetStore.getAll().size());

        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto3));

        Assert.assertEquals(1, targetStore.getAll().size());
        Assert.assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addrValue2)));
        Assert.assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(userNameField) && av.getStringValue().equals(usernameValue1)));
        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addrValue1)));
    }

    /**
     * Tests that validation in initiated for each derived target we add to the store.
     * One target is dependent of its parent target, one is independent (different flows).
     * @throws Exception may be thrown in startValidation.
     */
    @Test
    public void testValidationAfterInstantiatingDerivedTargets() throws Exception {
        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto1, dto2));

        Assert.assertEquals(2, targetStore.getAll().size());
        Mockito.verify(operationManager, times(2)).startValidation(Mockito.anyLong());
    }

    /**
     * Tests that readOnly attribute is populated correctly into {@link TargetSpec} from
     * {@link DerivedTargetSpecificationDTO}.
     */
    @Test
    public void testReadOnlyPopulation() {
        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto1));

        Assert.assertTrue(targetStore.getAll().get(0).getSpec().getReadOnly());

        derivedTargetParser.instantiateDerivedTargets(operationManager, parentTargetID1, Arrays.asList(dto4));

        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getReadOnly());
    }
}
