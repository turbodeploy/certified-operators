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

    private static final String name1 = "vmturbo";
    private static final String name2 = "ABC";

    private static final String addr1 = "1.2.3.4";
    private static final String addr2 = "250.250.250.250";

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
            .addTargetIdentifierField(nameField)
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
                    .setMandatory(true))
            .build();

    final DerivedTargetSpecificationDTO dto1 = DerivedTargetSpecificationDTO.newBuilder()
            .setProbeType(probeType1).setHidden(true).setDependent(true)
            .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                    .setStringValue(name1).build())
            .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                    .setStringValue(addr1).build())
            .build();

    final DerivedTargetSpecificationDTO dto2 = DerivedTargetSpecificationDTO.newBuilder()
            .setProbeType(probeType1).setHidden(false).setDependent(false)
            .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                    .setStringValue(name2).build())
            .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                    .setStringValue(addr2).build())
            .build();

    final DerivedTargetSpecificationDTO dto3 = DerivedTargetSpecificationDTO.newBuilder()
            .setProbeType(probeType1).setHidden(false).setDependent(true)
            .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                    .setStringValue(name1).build())
            .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                    .setStringValue(addr2).build())
            .build();

    final DerivedTargetSpecificationDTO dto4 = DerivedTargetSpecificationDTO.newBuilder()
            .setProbeType(probeType2).setHidden(false).setDependent(true)
            .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                    .setStringValue(name1).build())
            .addAccountValue(AccountValue.newBuilder().setKey(nameField)
                    .setStringValue(name2).build())
            .build();

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
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addr2)));
        Assert.assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(userNameField) && av.getStringValue().equals(name1)));
        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addr1)));
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
}
