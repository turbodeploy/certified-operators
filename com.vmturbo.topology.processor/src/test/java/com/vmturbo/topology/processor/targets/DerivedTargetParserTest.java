package com.vmturbo.topology.processor.targets;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.identity.exceptions.IdentityStoreException;
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

    private GroupScopeResolver groupScopeResolver;

    private TargetStore targetStore;

    private IdentityStore<TargetSpec> identityStore;

    private DerivedTargetParser derivedTargetParser;

    private long nextTargetId;

    @Before
    public void setup() throws Exception{
        keyValueStore = Mockito.mock(KeyValueStore.class);
        probeStore = Mockito.mock(ProbeStore.class);
        groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        identityStore = new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
        targetStore = new KVBackedTargetStore(keyValueStore, probeStore, identityStore,
                groupScopeResolver);
        derivedTargetParser = new DerivedTargetParser(probeStore, targetStore);
        nextTargetId = 10086L;
    }

    /**
     * Test instantiate derived targets.
     * @throws IdentityStoreException should never happen
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInstantiateDerivedTargets() throws IdentityStoreException {
        final String addressField = PredefinedAccountDefinition.Address.name().toLowerCase();
        final String userNameField = PredefinedAccountDefinition.Username.name().toLowerCase();
        final long probeId = 0L;
        final long parentTargetId = 0L;
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("test").setProbeType("foo")
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
        final DerivedTargetSpecificationDTO dto1 = DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType("foo").setHidden(true).setDependent(true)
                .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                        .setStringValue("vmturbo").build())
                .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                        .setStringValue("1.2.3.4").build())
                .build();
        final DerivedTargetSpecificationDTO dto2 = DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType("foo").setHidden(false).setDependent(true)
                .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                        .setStringValue("AAA").build())
                .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                        .setStringValue("250.250.250.250").build())
                .build();
        final List<DerivedTargetSpecificationDTO> derivedDTOList = Arrays.asList(dto1, dto2);

        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenReturn(Optional.of(probeId));

        derivedTargetParser.instantiateDerivedTargets(parentTargetId, derivedDTOList);

        Assert.assertEquals(2, targetStore.getAll().size());
        targetStore.getAll().stream().allMatch(target -> target.getSpec().hasParentId());
        targetStore.getAll().stream().anyMatch(target -> target.getSpec().getIsHidden());
    }

    /**
     * Tests old derived targets should be removed and new one will be created if parent target update the
     * returned DerivedTargetSpecificationDTO.
     */
    @Test
    public void testUpdateDerivedTargets() throws Exception {
        final String addressField = PredefinedAccountDefinition.Address.name().toLowerCase();
        final String userNameField = PredefinedAccountDefinition.Username.name().toLowerCase();
        final long probeId = 0L;
        final long parentTargetId = 0L;
        final String addr1 = "1.2.3.4";
        final String addr2 = "250.250.250.250";
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("test").setProbeType("foo")
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
        final DerivedTargetSpecificationDTO dto1 = DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType("foo").setHidden(true).setDependent(true)
                .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                        .setStringValue("vmturbo").build())
                .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                        .setStringValue(addr1).build())
                .build();
        final DerivedTargetSpecificationDTO dto2 = DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType("foo").setHidden(false).setDependent(true)
                .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                        .setStringValue("ABC").build())
                .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                        .setStringValue(addr2).build())
                .build();

        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenReturn(Optional.of(probeId));

        derivedTargetParser.instantiateDerivedTargets(parentTargetId, Arrays.asList(dto1, dto2));
        Assert.assertEquals(2, targetStore.getAll().size());

        final DerivedTargetSpecificationDTO dto3 = DerivedTargetSpecificationDTO.newBuilder()
                .setProbeType("foo").setHidden(true).setDependent(true)
                .addAccountValue(AccountValue.newBuilder().setKey(userNameField)
                        .setStringValue("AAA").build())
                .addAccountValue(AccountValue.newBuilder().setKey(addressField)
                        .setStringValue(addr2).build())
                .build();

        derivedTargetParser.instantiateDerivedTargets(parentTargetId, Arrays.asList(dto3));
        Assert.assertEquals(1, targetStore.getAll().size());
        Assert.assertTrue(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addr2)));
        Assert.assertFalse(targetStore.getAll().get(0).getSpec().getAccountValueList().stream()
                .anyMatch(av -> av.getKey().equals(addressField) && av.getStringValue().equals(addr1)));
    }

    private long nextTargetId() {
        return nextTargetId++;
    }
}
