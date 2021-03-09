package com.vmturbo.integrations.intersight.targetsync;

import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_ADDRESS;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_CLIENTID;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_CLIENTSECRET;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_PORT;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.AccountField;

/**
 * A factory class to mock {@link ProbeInfo} objects for testing.
 */
public class MockProbeInfo {

    private MockProbeInfo() {}

    /**
     * Create a {@link ProbeInfo} instance for the Intersight probe.
     *
     * @return the created {@link ProbeInfo} instance
     */
    public static ProbeInfo intersightProbeInfo() {
        return createMockProbeInfo(1, SDKProbeType.INTERSIGHT.getProbeType(),
                ProbeCategory.HYPERCONVERGED.getCategory(),
                ProbeCategory.HYPERCONVERGED.getCategory(), CreationMode.STAND_ALONE,
                INTERSIGHT_ADDRESS, createAccountDef(INTERSIGHT_ADDRESS),
                createAccountDef(INTERSIGHT_PORT, AccountFieldValueType.NUMERIC),
                createAccountDef(INTERSIGHT_CLIENTID), createAccountDef(INTERSIGHT_CLIENTSECRET)
        );
    }

    /**
     * Create a {@link ProbeInfo} instance with a single target id field.
     *
     * @param targetIdField the target id field name
     * @return the created {@link ProbeInfo} instance
     */
    public static ProbeInfo withSingleTargetIdField(String targetIdField) {
        return createMockProbeInfo(2, "type1", "category1", "category1",
                CreationMode.STAND_ALONE, targetIdField,
                createAccountDef("field0", AccountFieldValueType.BOOLEAN),
                createAccountDef(targetIdField),
                createAccountDef("field2", AccountFieldValueType.NUMERIC),
                createAccountDef("field3", AccountFieldValueType.STRING),
                createAccountDef("field4", AccountFieldValueType.LIST),
                createAccountDef("field5", AccountFieldValueType.GROUP_SCOPE)
        );
    }

    /**
     * Create a {@link ProbeInfo} instance based on the input.
     *
     * @param probeId the id of the probe
     * @param type the type of the probe
     * @param category the category of the probe
     * @param uiCategory the UI category of the probe
     * @param creationMode the creation mode of the probe
     * @param targetIdField the target id field name
     * @param entries the list of account entries of the probe
     * @return the created {@link ProbeInfo} instance
     */
    private static ProbeInfo createMockProbeInfo(long probeId, String type, String category,
            String uiCategory, CreationMode creationMode, String targetIdField,
            AccountDefEntry... entries) {
        final ProbeInfo newProbeInfo = Mockito.mock(ProbeInfo.class);
        when(newProbeInfo.getId()).thenReturn(probeId);
        when(newProbeInfo.getType()).thenReturn(type);
        when(newProbeInfo.getCategory()).thenReturn(category);
        when(newProbeInfo.getUICategory()).thenReturn(uiCategory);
        when(newProbeInfo.getAccountDefinitions()).thenReturn(Arrays.asList(entries));
        when(newProbeInfo.getCreationMode()).thenReturn(creationMode);
        when(newProbeInfo.getIdentifyingFields()).thenReturn(
                Collections.singletonList(targetIdField));
        return newProbeInfo;
    }

    /**
     * Create a {@link AccountDefEntry} instance given the key with the value type default to
     * {@link String}.
     *
     * @param key the key of the account definition
     * @return the created {@link AccountDefEntry} instance
     */
    private static AccountDefEntry createAccountDef(String key) {
        return createAccountDef(key, AccountFieldValueType.STRING);
    }

    /**
     * Create a {@link AccountDefEntry} instance given the key and value type.
     *
     * @param key the key of the account definition
     * @param valueType the value type of the account definition
     * @return the created {@link AccountDefEntry} instance
     */
    private static AccountDefEntry createAccountDef(String key, AccountFieldValueType valueType) {
        return new AccountField(key, key + "-name", key + "-description", true, false, false,
                valueType, null, Collections.emptyList(), ".*", null);
    }

}
