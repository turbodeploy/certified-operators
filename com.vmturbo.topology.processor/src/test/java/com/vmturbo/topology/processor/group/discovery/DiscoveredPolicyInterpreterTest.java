package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SERVICE_HORIZONTAL_SCALE_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.VCPU_RESIZE_THRESHOLD_DTO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.protobuf.util.JsonFormat;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test class for {@link DiscoveredSettingPolicyInterpreter}.
 */
public class DiscoveredPolicyInterpreterTest {

    private static final String VM_ASG_POLICY_NAME = "CSP:VMs_Accelerated Networking Enabled_EA - Development:1";
    private static final String CONTAINER_ASG_POLICY_NAME = "CSP:Deployment::btc/kubeturbo-btc-Kubernetes-btc AWS[Container]:1";
    private static final String VM_TEMPLATE_EXCLUSION_POLICY_NAME = "EXP:VMs_Accelerated Networking Enabled_EA - Development:1";

    @Mock
    private TargetStore targetStore;

    @Mock
    private EntityStore entityStore;

    private DiscoveredSettingPolicyInterpreter policyInterpreter;

    /**
     * Setup.
     */
    @Before
    public void setup() {

        // set up mocks
        MockitoAnnotations.initMocks(this);
        when(targetStore.getTargetDisplayName(TARGET_ID))
                .thenReturn(Optional.of("Test target " + TARGET_ID));
        when(entityStore.getTargetEntityIdMap(anyLong())).thenReturn(Optional.empty());

        policyInterpreter = new DiscoveredSettingPolicyInterpreter(targetStore, entityStore);
    }


    /**
     * Verify that groups with consistent scaling enabled generate the correct associated policies.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConvertConsistentScalingSettingsToPolicies() throws Exception {

        /*
         * Load some groups:
         * - Accelerated networking with consistent scaling set.  This will generate two policies.
         * - Consistent scaling only, enabled.  This will generate one policy.
         * - Consistent scaling only, disabled.  This will not generate a policy.
         *
         * Total number of policies generated will be two consistent scaling and one template
         * exclusion.
         */
        String[] groupFiles = {
                "AcceleratedNetworkingGroupDTO-consistent-scaling.json",  // VM
                "ConsistentScalingEnabled.json",  // CONTAINER
                "ConsistentScalingDisabled.json"  // CONTAINER
        };
        List<GroupDTO> groupDTOS = new ArrayList<>();
        for (String filename : groupFiles) {
            groupDTOS.add(loadGroupDto(filename));
        }

        final List<DiscoveredSettingPolicyInfo> policies = policyInterpreter.convertDiscoveredSettingPolicies(TARGET_ID, groupDTOS);
        Assert.assertEquals(3, policies.size());
        // Template exclusion policies are generated first, followed by consistent scaling.
        Iterator it = policies.iterator();
        DiscoveredSettingPolicyInfo policy = (DiscoveredSettingPolicyInfo)it.next();
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, policy.getEntityType());
        Assert.assertTrue(doesPolicyHaveSetting(policy, EntitySettingSpecs.ExcludedTemplates));
        Assert.assertEquals(1, countPoliciesWithSetting(policies,
                EntitySettingSpecs.ExcludedTemplates));
        // Check for consistent scaling policies
        Assert.assertEquals(2, countPoliciesWithSetting(policies,
                EntitySettingSpecs.EnableConsistentResizing));

        Set<String> policyNames = policies.stream().map(DiscoveredSettingPolicyInfo::getName)
                .collect(Collectors.toSet());
        Assert.assertThat(policyNames, containsInAnyOrder(VM_ASG_POLICY_NAME,
                CONTAINER_ASG_POLICY_NAME, VM_TEMPLATE_EXCLUSION_POLICY_NAME));
    }

    /**
     * Test method convertTemplateExclusionGroupsToPolicies.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConvertTemplateExclusionGroupsToPolicies() throws Exception {
        // Test against the Accelerated Networking group
        final GroupDTO group = loadGroupDto("AcceleratedNetworkingGroupDTO.json");

        List<DiscoveredSettingPolicyInfo> policies = policyInterpreter.convertDiscoveredSettingPolicies(
                TARGET_ID, ImmutableList.of(group));


        Assert.assertTrue(!policies.isEmpty());
        DiscoveredSettingPolicyInfo policy = policies.iterator().next();
        String name = "VMs_Accelerated Networking Enabled_EA - Development";
        Assert.assertThat(policy.getName(), CoreMatchers.containsString(name));
        Assert.assertThat(policy.getDisplayName(), CoreMatchers.containsString(name));
        Assert.assertThat(policy.getDiscoveredGroupNames(0), CoreMatchers.containsString(name));
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, policy.getEntityType());
        Assert.assertTrue(doesPolicyHaveSetting(policy, EntitySettingSpecs.ExcludedTemplates));
    }

    /**
     * Test converting groups with SLOHorizontalScale setting policy.
     */
    @Test
    public void testConvertSLOHorizontalScaleSettingPolicy() {
        // ACT
        final List<DiscoveredSettingPolicyInfo> settingPolicyInfos = policyInterpreter.convertDiscoveredSettingPolicies(
                TARGET_ID, ImmutableList.of(SERVICE_HORIZONTAL_SCALE_DTO));

        // ASSERTS
        assertEquals(1, settingPolicyInfos.size());
        final DiscoveredSettingPolicyInfo policySettingInfo = settingPolicyInfos.get(0);
        assertEquals("SET:47d663e2-9014-4fda-800a-ee83da4a9d18:1", policySettingInfo.getName());
        assertEquals(EntityType.SERVICE_VALUE, policySettingInfo.getEntityType());
        assertEquals(1, policySettingInfo.getDiscoveredGroupNamesCount());
        assertEquals("0-47d663e2-9014-4fda-800a-ee83da4a9d18",
                policySettingInfo.getDiscoveredGroupNames(0));
        assertEquals("SLOHorizontalScale on turbonomic/policy-binding-sample [Kubernetes-PT-K8S] (target 1)",
                policySettingInfo.getDisplayName());
        assertEquals(8, policySettingInfo.getSettingsCount());
        final Optional<Setting> minReplicas = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.MinReplicas.getSettingName()))
                .findAny();
        assertTrue(minReplicas.isPresent());
        assertEquals(3, minReplicas.get().getNumericSettingValue().getValue(), 0.001);
        final Optional<Setting> maxReplicas = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.MaxReplicas.getSettingName()))
                .findAny();
        assertTrue(maxReplicas.isPresent());
        assertEquals(10, maxReplicas.get().getNumericSettingValue().getValue(), 0.001);
        final Optional<Setting> responseTimeSLO = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.ResponseTimeSLO.getSettingName()))
                .findAny();
        assertTrue(responseTimeSLO.isPresent());
        assertEquals(300.1, responseTimeSLO.get().getNumericSettingValue().getValue(), 0.001);
        final Optional<Setting> transactionSLO = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.TransactionSLO.getSettingName()))
                .findAny();
        assertTrue(transactionSLO.isPresent());
        assertEquals(20, transactionSLO.get().getNumericSettingValue().getValue(), 0.001);
        final Optional<Setting> scaleUp = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName()
                        .equals(ConfigurableActionSettings.HorizontalScaleUp.getSettingName()))
                .findAny();
        assertTrue(scaleUp.isPresent());
        assertEquals("Automatic", scaleUp.get().getEnumSettingValue().getValue());
        final Optional<Setting> scaleDown = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName()
                        .equals(ConfigurableActionSettings.HorizontalScaleDown.getSettingName()))
                .findAny();
        assertTrue(scaleDown.isPresent());
        assertEquals("Disabled", scaleDown.get().getEnumSettingValue().getValue());
        final Optional<Setting> transactionSLOEnabled = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.TransactionSLOEnabled.getSettingName()))
                .findAny();
        assertTrue(transactionSLOEnabled.isPresent());
        assertTrue(transactionSLOEnabled.get().getBooleanSettingValue().getValue());
        final Optional<Setting> responseTimeSLOEnabled = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName()))
                .findAny();
        assertTrue(responseTimeSLOEnabled.isPresent());
        assertTrue(responseTimeSLOEnabled.get().getBooleanSettingValue().getValue());
    }

    /**
     * Test the conversion of groups with a setting policy associated with it that specifies
     * vcpu resize min/max thresholds.
     */
    @Test
    public void testConvertVcpuResizeThresholdSettingPolicy() {

        // ASSERT
        final List<DiscoveredSettingPolicyInfo> settingPolicyInfos = policyInterpreter.convertDiscoveredSettingPolicies(
                TARGET_ID, ImmutableList.of(DiscoveredGroupConstants.VCPU_RESIZE_THRESHOLD_DTO));

        assertThat(settingPolicyInfos.size(), is(1));
        final DiscoveredSettingPolicyInfo policySettingInfo = settingPolicyInfos.get(0);
        assertThat(policySettingInfo.getName(), is("SET:group:1"));
        assertThat(policySettingInfo.getEntityType(), is(EntityType.VIRTUAL_MACHINE_VALUE));
        assertThat(policySettingInfo.getDiscoveredGroupNamesCount(), is(1));
        assertThat(policySettingInfo.getDiscoveredGroupNames(0), is("0-group"));
        assertThat(policySettingInfo.getDisplayName(),
                is("Freedom is slavery. - Setting policy (target 1)"));
        assertThat(policySettingInfo.getSettingsCount(), is(2));
        final Optional<SettingProto.Setting> minSetting = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs
                        .ResizeVcpuMinThreshold
                        .getSettingName()))
                .findAny();
        assertTrue(minSetting.isPresent());
        assertThat((double)minSetting.get().getNumericSettingValue().getValue(),
                closeTo(4.0, 0.001));

        final Optional<SettingProto.Setting> maxSetting = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs
                        .ResizeVcpuMaxThreshold
                        .getSettingName()))
                .findAny();
        assertTrue(maxSetting.isPresent());
        assertThat((double)maxSetting.get().getNumericSettingValue().getValue(),
                closeTo(16.0, 0.001));
    }

    /**
     * Same as {@link #testConvertVcpuResizeThresholdSettingPolicy()} but has a policy
     * display name.
     */
    @Test
    public void testVcpuResizeWithPolicyDisplayName() {

        // ACT
        final CommonDTO.GroupDTO groupDTO = DiscoveredGroupConstants.VCPU_RESIZE_THRESHOLD_DTO.toBuilder()
                .setSettingPolicy(VCPU_RESIZE_THRESHOLD_DTO.getSettingPolicy().toBuilder()
                        .setDisplayName("DO NOT USE GROUP NAME")
                        .build())
                .build();
        final List<DiscoveredSettingPolicyInfo> settingPolicyInfos = policyInterpreter.convertDiscoveredSettingPolicies(
                TARGET_ID, ImmutableList.of(groupDTO));

        // ASSERT
        assertThat(settingPolicyInfos.size(), is(1));
        final DiscoveredSettingPolicyInfo policySettingInfo = settingPolicyInfos.get(0);
        assertThat(policySettingInfo.getName(), is("SET:group:1"));
        assertThat(policySettingInfo.getEntityType(), is(EntityType.VIRTUAL_MACHINE_VALUE));
        assertThat(policySettingInfo.getDiscoveredGroupNamesCount(), is(1));
        assertThat(policySettingInfo.getDiscoveredGroupNames(0), is("0-group"));
        assertThat(policySettingInfo.getDisplayName(),
                is("DO NOT USE GROUP NAME (target 1)"));
        assertThat(policySettingInfo.getSettingsCount(), is(2));
        final Optional<SettingProto.Setting> minSetting = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs
                        .ResizeVcpuMinThreshold
                        .getSettingName()))
                .findAny();
        assertTrue(minSetting.isPresent());
        assertThat((double)minSetting.get().getNumericSettingValue().getValue(),
                closeTo(4.0, 0.001));

        final Optional<SettingProto.Setting> maxSetting = policySettingInfo.getSettingsList()
                .stream()
                .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs
                        .ResizeVcpuMaxThreshold
                        .getSettingName()))
                .findAny();
        assertTrue(maxSetting.isPresent());
        assertThat((double)maxSetting.get().getNumericSettingValue().getValue(),
                closeTo(16.0, 0.001));
    }

    /**
     * Check whether a policy contains the specified setting.
     * @param policy policy to check
     * @param setting setting to check for
     * @return true if the policy contains the setting
     */
    private boolean doesPolicyHaveSetting(DiscoveredSettingPolicyInfo policy,
                                          EntitySettingSpecs setting) {
        return policy.getSettingsList().stream()
                .anyMatch(s -> s.getSettingSpecName() == setting.getSettingName());
    }

    /**
     * Return the number of policies that contain the specified setting.
     * @param policies list of policies to check
     * @param setting setting to check for
     * @return the number of policies with the specified setting
     */
    private long countPoliciesWithSetting(final List<DiscoveredSettingPolicyInfo> policies,
                                          final EntitySettingSpecs setting) {
        return policies.stream().filter(p -> doesPolicyHaveSetting(p, setting)).count();
    }


    /**
     * Load DTO from a JSON file.
     *
     * @param jsonFileName file name
     * @return action DTO
     * @throws IOException error reading the file
     */
    private GroupDTO loadGroupDto(@Nonnull String jsonFileName) throws IOException {

        String str = readResourceFileAsString(jsonFileName);
        GroupDTO.Builder builder = GroupDTO.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    private String readResourceFileAsString(String fileName) throws IOException {
        File file = ResourcePath.getTestResource(getClass(), fileName).toFile();
        return Files.asCharSource(file, Charset.defaultCharset()).read();
    }
}
