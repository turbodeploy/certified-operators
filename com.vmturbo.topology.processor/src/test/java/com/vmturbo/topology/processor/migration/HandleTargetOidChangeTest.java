package com.vmturbo.topology.processor.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.AccountDefEntryConstants;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Test the functionality of the helper class HandleTargetOidChange.
 */
public class HandleTargetOidChangeTest {

    private static final AccountDefEntry ADDRESS_FIELD =
            Probes.createStringAccountDefinition(AccountDefEntryConstants.ADDRESS_FIELD).build();

    private static final String OTHER_FIELD_NAME = "OTHER";

    private static final AccountDefEntry OTHER_FIELD =
            Probes.createStringAccountDefinition(OTHER_FIELD_NAME).build();

    private static final ProbeInfo PROBE1_INFO = Probes.createEmptyProbe()
            .addAccountDefinition(ADDRESS_FIELD)
            .addAccountDefinition(OTHER_FIELD)
            .addTargetIdentifierField(AccountDefEntryConstants.ADDRESS_FIELD)
            .build();

    private static final ProbeInfo PROBE2_INFO = Probes.createEmptyProbe()
            .addAccountDefinition(ADDRESS_FIELD)
            .addAccountDefinition(OTHER_FIELD)
            .addTargetIdentifierField(OTHER_FIELD_NAME)
            .build();

    private static final long PROBE1_ID = 1234L;

    private static final long PROBE2_ID = 1235L;

    private static final long TARGET1_ID = 1236L;

    private static final long TARGET2_ID = 1237L;

    private static final long TARGET3_ID = 1238L;

    private static final String ADDRESS_VALUE = "MixedCaseAddress";

    private static final String OTHER_VALUE = "OTHER-VALUE";

    private static final TargetSpec TARGET1_SPEC_WITH_ADDRESS_AS_IDENTIFYING_FIELD =
            TargetSpec.newBuilder()
                    .setProbeId(PROBE1_ID)
                    .addAccountValue(AccountValue.newBuilder()
                            .setKey(AccountDefEntryConstants.ADDRESS_FIELD)
                            .setStringValue(ADDRESS_VALUE)
                            .build())
                    .build();

    private static final TargetSpec TARGET2_SPEC_WITH_ADDRESS_AS_IDENTIFYING_FIELD =
            TargetSpec.newBuilder()
                    .setProbeId(PROBE1_ID)
                    .addAccountValue(AccountValue.newBuilder()
                            .setKey(AccountDefEntryConstants.ADDRESS_FIELD)
                            .setStringValue(ADDRESS_VALUE.toUpperCase())
                            .build())
                    .build();

    private static final TargetSpec TARGET3_SPEC_WITH_OTHER_AS_IDENTIFYING_FIELD =
            TargetSpec.newBuilder()
                    .setProbeId(PROBE1_ID)
                    .addAccountValue(AccountValue.newBuilder()
                            .setKey(AccountDefEntryConstants.ADDRESS_FIELD)
                            .setStringValue(ADDRESS_VALUE)
                            .build())
                    .addAccountValue(AccountValue.newBuilder()
                            .setKey(OTHER_FIELD_NAME)
                            .setStringValue(OTHER_VALUE)
                            .build())
                    .build();

    private static final Target TARGET1 = mock(Target.class);

    private static final Target TARGET2 = mock(Target.class);

    private static final Target TARGET3 = mock(Target.class);

    private final ProbeStore probeStore = mock(RemoteProbeStore.class);

    private final TargetStore targetStore = mock(CachingTargetStore.class);

    private final IdentityStore<TargetSpec> idStore =
            new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));

    private final IdentityStore<TargetSpec> idStoreSpy = spy(idStore);

    private final TargetDao targetDao = mock(TargetDao.class);

    private final HandleTargetOidChange migration = getMigrationWithAccountFields(Collections
                    .singleton(AccountDefEntryConstants.ADDRESS_FIELD));

    private HandleTargetOidChange getMigrationWithAccountFields(
            @Nonnull Set<String> fields) {
        return new HandleTargetOidChange(probeStore,
                targetStore, idStoreSpy, targetDao) {
            @Override
            protected Set<String> getImpactedAccountDefKeys() {
                return fields;
            }
        };
    }

    @Captor
    private ArgumentCaptor<Set<Long>> removeCaptor;

    @Captor
    private ArgumentCaptor<Map<Long, TargetSpec>> updateAttributesCaptor;

    /**
     * Setup the mocks.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(probeStore.getProbes()).thenReturn(ImmutableMap.of(PROBE1_ID, PROBE1_INFO,
                PROBE2_ID, PROBE2_INFO));
        when(probeStore.getProbe(PROBE1_ID)).thenReturn(Optional.of(PROBE1_INFO));
        when(probeStore.getProbe(PROBE2_ID)).thenReturn(Optional.of(PROBE2_INFO));
        when(TARGET1.getProbeInfo()).thenReturn(PROBE1_INFO);
        when(TARGET2.getProbeInfo()).thenReturn(PROBE1_INFO);
        when(TARGET3.getProbeInfo()).thenReturn(PROBE2_INFO);
        when(targetStore.getProbeTargets(PROBE1_ID)).thenReturn(ImmutableList.of(TARGET1, TARGET2));
        when(targetStore.getProbeTargets(PROBE2_ID)).thenReturn(ImmutableList.of(TARGET3));
        when(TARGET1.getId()).thenReturn(TARGET1_ID);
        when(TARGET2.getId()).thenReturn(TARGET2_ID);
        when(TARGET3.getId()).thenReturn(TARGET3_ID);
        when(TARGET1.getSpec()).thenReturn(TARGET1_SPEC_WITH_ADDRESS_AS_IDENTIFYING_FIELD);
        when(TARGET2.getSpec()).thenReturn(TARGET2_SPEC_WITH_ADDRESS_AS_IDENTIFYING_FIELD);
        when(TARGET3.getSpec()).thenReturn(TARGET3_SPEC_WITH_OTHER_AS_IDENTIFYING_FIELD);
    }

    /**
     * Test the normal flow and that duplicate targets are removed.
     *
     * @throws Exception when IdentityStore does.
     */
    @Test
    public void testDuplicateRemoved() throws Exception {
        migration.doStartMigration();
        verify(idStoreSpy).removeItemOids(removeCaptor.capture());
        // we will remove 1 temporary Oid we created for target1 and target2 (they now map to the
        // same oid) and also remove the original oid of target2.
        assertEquals(2, removeCaptor.getValue().size());
        assertTrue(removeCaptor.getValue().contains(TARGET2_ID));
        assertFalse(removeCaptor.getValue().contains(TARGET1_ID));
        assertFalse(removeCaptor.getValue().contains(TARGET3_ID));
        verify(idStoreSpy).updateItemAttributes(updateAttributesCaptor.capture());
        assertEquals(1, updateAttributesCaptor.getAllValues().size());
        assertEquals(TARGET1_SPEC_WITH_ADDRESS_AS_IDENTIFYING_FIELD,
                updateAttributesCaptor.getValue().get(TARGET1_ID));
        verify(targetDao).remove(TARGET2_ID);
    }
}
