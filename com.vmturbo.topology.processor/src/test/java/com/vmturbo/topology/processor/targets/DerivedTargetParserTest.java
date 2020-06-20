package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Test the {@link DerivedTargetParser}.
 */
public class DerivedTargetParserTest {

    private TargetStore targetStore;

    private DerivedTargetParser derivedTargetParser;

    @Before
    public void setup() throws Exception {
        TargetDao targetDao = mock(TargetDao.class);
        ProbeStore probeStore = mock(ProbeStore.class);
        IdentityStore<TargetSpec> identityStore =
            new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
        targetStore = new CachingTargetStore(targetDao, probeStore, identityStore);
        derivedTargetParser = new DerivedTargetParser(probeStore, targetStore);
        when(probeStore.getProbe(probeID1)).thenReturn(Optional.of(probeInfo1));
        when(probeStore.getProbeIdForType(probeType1)).thenReturn(Optional.of(probeID1));
        when(probeStore.getProbe(probeID2)).thenReturn(Optional.of(probeInfo2));
        when(probeStore.getProbeIdForType(probeType2)).thenReturn(Optional.of(probeID2));
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
    private static final String parentAddress = "5.6.7.8";
    private static final String addrValue1 = "1.2.3.4";
    private static final String addrValue2 = "250.250.250.250";

    final ProbeInfo probeInfo1 = ProbeInfo.newBuilder()
            .setProbeCategory(probeCategory).setUiProbeCategory(probeCategory).setProbeType(probeType1)
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
            .setProbeCategory(probeCategory).setUiProbeCategory(probeCategory).setProbeType(probeType2)
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

    private Target createParentTarget()
        throws DuplicateTargetException, InvalidTargetException, IdentityStoreException {
        return targetStore.createTarget(TargetSpec.newBuilder()
            .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                .setKey(addressField)
                .setStringValue(parentAddress))
            .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                .setKey(userNameField)
                .setStringValue(usernameValue1))
            .setProbeId(probeID1)
            .build());
    }

    /**
     * Test that derived targets are properly created with hidden and dependent properly taken into
     * account.
     *
     * @throws DuplicateTargetException when targetStore does.
     * @throws InvalidTargetException when targetStore does.
     * @throws IdentityStoreException when targetStore does.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInstantiateDerivedTargetsForAddressField()
        throws DuplicateTargetException, InvalidTargetException, IdentityStoreException {
        Target parent = createParentTarget();
        derivedTargetParser.instantiateDerivedTargets(parent.getId(), Arrays.asList(dto1, dto2));

        assertEquals(3, targetStore.getAll().size());
        Optional<Target> updatedParent = targetStore.getTarget(parent.getId());
        assertTrue(updatedParent.isPresent());
        Set<String> derivedTargetDisplayNames = targetStore.getDerivedTargetIds(parent.getId())
            .stream()
            .map(targetStore::getTarget)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(Target::getDisplayName)
            .collect(Collectors.toSet());
        assertEquals(1, derivedTargetDisplayNames.size());
        assertEquals(addrValue1, derivedTargetDisplayNames.iterator().next());
        assertTrue(targetStore.getAll().stream()
            .anyMatch(target -> target.getDisplayName().equals(addrValue1)));
        assertTrue(targetStore.getAll().stream()
            .anyMatch(target -> target.getDisplayName().equals(addrValue2)));
        assertTrue(targetStore.getAll().stream()
            .filter(target -> target.getDisplayName().equals(addrValue1))
            .anyMatch(target -> target.getSpec().getIsHidden()));
        assertTrue(targetStore.getAll().stream()
            .filter(target -> target.getDisplayName().equals(addrValue2))
            .anyMatch(target -> !target.getSpec().getIsHidden()));
    }

    /**
     * Test instantiation of derived targets using name field instead of address field.
     *
     * @throws DuplicateTargetException when targetStore does.
     * @throws InvalidTargetException when targetStore does.
     * @throws IdentityStoreException when targetStore does.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInstantiateDerivedTargetsForNameField()
        throws DuplicateTargetException, InvalidTargetException, IdentityStoreException {
        Target parent = createParentTarget();
        derivedTargetParser.instantiateDerivedTargets(parent.getId(), Arrays.asList(dto4));

        assertEquals(2, targetStore.getAll().size());
        Target updatedParent = targetStore.getTarget(parent.getId()).get();
        assertEquals(1, targetStore.getDerivedTargetIds(updatedParent.getId()).size());
        Optional<Target> derivedTarget = targetStore.getTarget(
            targetStore.getDerivedTargetIds(updatedParent.getId()).iterator().next());
        assertTrue(derivedTarget.isPresent());
        assertTrue(!derivedTarget.get().getSpec().getIsHidden());
        assertEquals(nameValue, derivedTarget.get().getDisplayName());
    }

    /**
     * Tests old derived targets should be removed and new one will be created if parent target update the
     * returned DerivedTargetSpecificationDTO.
     */
    @Test
    public void testUpdateDerivedTargets() {
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto1, dto2));

        assertEquals(2, targetStore.getAll().size());

        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto3));

        assertEquals(1, targetStore.getAll().size());
        assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addrValue2)));
        assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(userNameField) && av.getStringValue().equals(usernameValue1)));
        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addrValue1)));
    }

    /**
     * Tests that existing, derived, dependent targets should be removed if parent target removes
     * all derived targets.
     */
    @Test
    public void testDeleteAllDependentDerivedTargets() {
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto1, dto3));
        assertEquals(2, targetStore.getAll().size());
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Collections.emptyList());
        assertEquals(0, targetStore.getAll().size());
    }

    /**
     * Tests that existing, derived, independent targets are not removed if parent target removes
     * all derived targets.
     */
    @Test
    public void testDeleteAllIndependentDerivedTargets() {
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto2));
        assertEquals(1, targetStore.getAll().size());
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Collections.emptyList());
        assertEquals(1, targetStore.getAll().size());
    }

    /**
     * Tests that readOnly attribute is populated correctly into {@link TargetSpec} from
     * {@link DerivedTargetSpecificationDTO}.
     */
    @Test
    public void testReadOnlyPopulation() {
        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto1));

        assertTrue(targetStore.getAll().get(0).getSpec().getReadOnly());

        derivedTargetParser.instantiateDerivedTargets(parentTargetID1, Arrays.asList(dto4));

        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getReadOnly());
    }
}
