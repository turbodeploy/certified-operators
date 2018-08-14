package com.vmturbo.topology.processor.targets;

import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Test the {@link DerivedTargetParser}.
 */
public class DerivedTargetParserTest {

    private KeyValueStore keyValueStore;

    private IdentityProvider identityProvider;

    private ProbeStore probeStore;

    private TargetStore targetStore;

    private DerivedTargetParser derivedTargetParser;

    private long nextTargetId;

    @Before
    public void setup() throws Exception{
        keyValueStore = Mockito.mock(KeyValueStore.class);
        identityProvider = Mockito.mock(IdentityProvider.class);
        probeStore = Mockito.mock(ProbeStore.class);
        targetStore = new KVBackedTargetStore(keyValueStore, identityProvider, probeStore);
        derivedTargetParser = new DerivedTargetParser(probeStore, targetStore);
        nextTargetId = 10086L;
    }

    /**
     * Test instantiate derived targets.
     */
    @Test
    public void testInstantiateDerivedTargets() {
        final String addressField = PredefinedAccountDefinition.Address.name().toLowerCase();
        final String userNameField = PredefinedAccountDefinition.Username.name().toLowerCase();
        final long probeId = 0L;
        final long parentTargetId = 0L;
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("test").setProbeType("foo").addTargetIdentifierField("BBB")
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

        Mockito.when(identityProvider.getTargetId(any())).thenAnswer(new Answer<Long>() {
            @Override public Long answer(InvocationOnMock invocation) throws Throwable {
                return nextTargetId();
            }
        });
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
        Mockito.when(identityProvider.getTargetId(any())).thenAnswer(new Answer<Long>() {
            @Override public Long answer(InvocationOnMock invocation) throws Throwable {
                return nextTargetId();
            }
        });

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
