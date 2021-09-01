package com.vmturbo.integrations.intersight.targetsync;

import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_ADDRESS;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_CLIENTID;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_CLIENTSECRET;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_PORT;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.cisco.intersight.client.model.AssetTarget;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

/**
 * Test cases for {@link IntersightTargetConverter}.
 */
public class IntersightTargetConverterTest {
    /**
     * Test the InputFields method for asset targets.
     */
    @Test
    public void testAssetTargetInputFields() {
        final String targetMoid = "foo";
        final AssetTarget assetTarget = MockAssetTarget.withTargetMoid(targetMoid);
        final String targetIdField = "what";
        final ProbeInfo probeInfo = MockProbeInfo.withSingleTargetIdField(targetIdField);

        // without assist
        final TargetInputFields inputFieldsWithoutAssist =
                IntersightTargetConverter.inputFields(assetTarget, Optional.empty(), probeInfo);
        Assert.assertEquals(Optional.empty(), inputFieldsWithoutAssist.getCommunicationBindingChannel());

        // with assist
        final String assistId = "bar";
        final TargetInputFields inputFieldsWithAssist =
                IntersightTargetConverter.inputFields(assetTarget, Optional.of(assistId), probeInfo);
        Assert.assertEquals(Optional.of(assistId), inputFieldsWithAssist.getCommunicationBindingChannel());

        // check account values
        final Set<AccountValue> accountValues = inputFieldsWithAssist.getAccountData();
        Assert.assertNotNull(accountValues);
        // Assert all target id fields are populated with the asset target MOID
        final List<String> targetIdFieldValues = getFieldValues(accountValues, targetIdField);
        Assert.assertTrue(targetIdFieldValues.size() > 0);
        Assert.assertTrue(targetIdFieldValues.stream().allMatch(targetMoid::equals));
    }

    /**
     * Test the InputFields method for asset targets.
     */
    @Test
    public void testIntersightTargetInputFields() {
        final String address = "bullwinkle";
        final int port = 7979;
        final String clientId = "foo";
        final String clientSecret = "bar";
        final IntersightConnection connection = new IntersightConnection(address, port, clientId,
                clientSecret);
        final ProbeInfo intersightProbeInfo = MockProbeInfo.intersightProbeInfo();
        final TargetInputFields inputFields =
                IntersightTargetConverter.inputFields(connection, intersightProbeInfo);
        final Set<AccountValue> accountValues = inputFields.getAccountData();
        Assert.assertNotNull(accountValues);
        final List<String> addresses = getFieldValues(accountValues, INTERSIGHT_ADDRESS);
        Assert.assertEquals(1, addresses.size());
        Assert.assertEquals(address, addresses.get(0));
        final List<String> ports = getFieldValues(accountValues, INTERSIGHT_PORT);
        Assert.assertEquals(1, ports.size());
        Assert.assertEquals(String.valueOf(port), ports.get(0));
        final List<String> clientIds = getFieldValues(accountValues, INTERSIGHT_CLIENTID);
        Assert.assertEquals(1, clientIds.size());
        Assert.assertEquals(clientId, clientIds.get(0));
        final List<String> clientSecrets = getFieldValues(accountValues, INTERSIGHT_CLIENTSECRET);
        Assert.assertEquals(1, clientSecrets.size());
        Assert.assertEquals(clientSecret, clientSecrets.get(0));
    }

    /**
     * Retrieve the value of a specified field from the input set of {@link AccountValue}s.
     *
     * @param accountValues the set of {@link AccountValue}s
     * @param fieldName     the input field name
     * @return the found set of {@link String} values matching the field
     */
    private static List<String> getFieldValues(final Set<AccountValue> accountValues,
                                               final String fieldName) {
        return accountValues.stream().filter(av -> fieldName.equals(av.getName()))
                .map(AccountValue::getStringValue).collect(Collectors.toList());
    }

    /**
     * Test get converted MO id method for asset targets.
     */
    @Test
    public void testGetConvertedTargetMoId() {
        // regular conversion for non-newrelic target
        final String targetMoid = "61250b4a7564612d3315db29";
        final AssetTarget assetTargetNonNewRelic = MockAssetTarget.withTargetMoidAndType(
                targetMoid, AssetTarget.TargetTypeEnum.DYNATRACE);
        Assert.assertEquals(IntersightTargetConverter.getConvertedTargetMoId(assetTargetNonNewRelic),
                targetMoid);

        // Hex to decimal integer literal conversionfor newrelic target.
        final AssetTarget assetTargetNewRelic = MockAssetTarget.withTargetMoidAndType(
                targetMoid, AssetTarget.TargetTypeEnum.NEWRELIC);
        Assert.assertEquals(IntersightTargetConverter.getConvertedTargetMoId(assetTargetNewRelic),
                "30064829527545578813239188265");

        // Null is expected for Invalid moid conversion for newrelic target.
        final AssetTarget assetTargetNewRelic1 = MockAssetTarget.withTargetMoidAndType(
                "ABCDEFRGSJSJ123", AssetTarget.TargetTypeEnum.NEWRELIC);
        Assert.assertNull(IntersightTargetConverter.getConvertedTargetMoId(assetTargetNewRelic1));
    }

}
