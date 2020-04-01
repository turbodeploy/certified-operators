package com.vmturbo.topology.processor.rest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopePropertySet;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.actions.SdkToProbeActionsConverter;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.AccountField;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.GetAllProbes;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

/**
 * Tests for the REST interface for probes.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context with no probes/targets registered.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public final class ProbeControllerTest {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public ProbeStore probeStore() {
            return Mockito.mock(ProbeStore.class);
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(GSON);
            converters.add(msgConverter);
        }

        @Bean
        public ProbeController probeController() {
            return new ProbeController(probeStore());
        }
    }

    private static MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;

    private ProbeStore probeStore;

    private AccountDefEntry optionalEntry;
    private AccountDefEntry mandatoryEntry;

    private ProbeInfo oneAccountFieldProbe;
    private ProbeInfo twoAccountFieldsProbe;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

        probeStore = wac.getBean(ProbeStore.class);

        optionalEntry = AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                        .setName("name")
                        .setDisplayName("displayName")
                        .setDescription("desc")
                        .setPrimitiveValue(PrimitiveValue.STRING))
                .setMandatory(false)
                .build();

        mandatoryEntry = AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                        .setName("name2")
                        .setDisplayName("displayName2")
                        .setDescription("desc2")
                        .setPrimitiveValue(PrimitiveValue.STRING))
                .setMandatory(false)
                .build();

        oneAccountFieldProbe = ProbeInfo.newBuilder()
                .setProbeType("type")
                .setProbeCategory("cat")
                .setUiProbeCategory("uiCat")
                .addAccountDefinition(optionalEntry)
                .addTargetIdentifierField("name")
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                                .setActionType(ActionType.START)
                                .build())
                        .build())
                .build();

        twoAccountFieldsProbe = ProbeInfo.newBuilder()
                .setProbeType("type2")
                .setProbeCategory("cat2")
                .setUiProbeCategory("uiCat2")
                .addAccountDefinition(optionalEntry)
                .addAccountDefinition(mandatoryEntry)
                .addTargetIdentifierField("name")
                .build();
    }

    /**
     * Test that a {@link ProbeDescription} accurately represents the
     * {@link ProbeInfo} it was constructed from.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void testProbeDescription() throws Exception {
        final ProbeDescription oneFieldDesc = ProbeController.create(0L, oneAccountFieldProbe);
        final ProbeDescription twoFieldDesc = ProbeController.create(0, twoAccountFieldsProbe);

        checkProbeDescription(oneAccountFieldProbe, oneFieldDesc);
        checkProbeDescription(twoAccountFieldsProbe, twoFieldDesc);

        exception.expect(ComparisonFailure.class);
        checkProbeDescription(twoAccountFieldsProbe, oneFieldDesc);
    }

    /**
     * Test that a {@link AccountField} accurately represents the
     * {@link AccountDefEntry} it was constructed from.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void testAccountField() throws Exception {
        final AccountField optionalField = ProbeController.create(optionalEntry);
        final AccountField mandatoryField = ProbeController.create(mandatoryEntry);

        checkAccountField(optionalEntry, optionalField);
        checkAccountField(mandatoryEntry, mandatoryField);

        exception.expect(ComparisonFailure.class);
        checkAccountField(mandatoryEntry, optionalField);
    }

    /**
     * Test that a GET succeeds even when there are no probes.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void getAllEmpty() throws Exception {
        Mockito.when(probeStore.getProbes()).thenReturn(new HashMap<>());
        Assert.assertTrue(getAllProbes().isEmpty());
    }

    /**
     * Test that a GET succeeds when there is one probe, and
     * the returned {@link ProbeDescription} accurately
     * represents the probe.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void getAllOneProbe() throws Exception {
        final Map<Long, ProbeInfo> retMap = new HashMap<>();
        final long id = 1;
        retMap.put(id, oneAccountFieldProbe);
        Mockito.when(probeStore.getProbes()).thenReturn(retMap);
        final List<ProbeDescription> descriptions = getAllProbes();
        Assert.assertEquals(1, descriptions.size());
        final ProbeDescription desc = descriptions.get(0);
        Assert.assertEquals(id, desc.getId());
        checkProbeDescription(retMap.get(id), desc);
    }

    /**
     * Test that a GET succeeds when there is one probe with
     * two {@link AccountDefEntry} entries.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void getAllOneProbeTwoAccountFields() throws Exception {
        final Map<Long, ProbeInfo> retMap = new HashMap<>();
        final long id = 1;
        retMap.put(id, twoAccountFieldsProbe);

        Mockito.when(probeStore.getProbes()).thenReturn(retMap);

        final List<ProbeDescription> descriptions = getAllProbes();
        Assert.assertEquals(1, descriptions.size());
        final ProbeDescription desc = descriptions.get(0);
        Assert.assertEquals(id, desc.getId());
        checkProbeDescription(retMap.get(id), desc);
    }

    /**
     * Test that a GET succeeds even when there are two probes.
     *
     * @throws Exception If exceptions occur.
     */
    @Test
    public void getAllTwoProbes() throws Exception {
        final Map<Long, ProbeInfo> retMap = new HashMap<>();
        final long[] ids = new long[]{1, 2};
        retMap.put(ids[0], oneAccountFieldProbe);
        retMap.put(ids[1], twoAccountFieldsProbe);

        Mockito.when(probeStore.getProbes()).thenReturn(retMap);

        final List<ProbeDescription> descriptions = getAllProbes();
        Assert.assertEquals(2, descriptions.size());
        IntStream.range(0, 2).forEach(i -> {
            final ProbeDescription desc = descriptions.get(i);
            Assert.assertEquals(ids[i], desc.getId());
            checkProbeDescription(retMap.get(ids[i]), desc);
        });
    }

    /**
     * Tests getting probe though the REST interface.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testGetProbe() throws Exception {
        final long probeId1 = 1123124;
        final long probeId2 = 112312433;
        final ProbeInfo probeInfo1 = populateProbeInfo(oneAccountFieldProbe, "type1", "category2",
                Collections.emptyList()).build();
        final ProbeInfo probeInfo2 = populateProbeInfo(oneAccountFieldProbe, "type3", "category5",
                Collections.emptyList()).build();
        Mockito.when(probeStore.getProbe(probeId1)).thenReturn(Optional.of(probeInfo1));
        Mockito.when(probeStore.getProbe(probeId2)).thenReturn(Optional.of(probeInfo2));
        final ProbeDescription resp = parseProbeDescription(probeId1);
        Assert.assertEquals(probeInfo1.getProbeType(), resp.getType());
    }

    /**
     * Tests different account value types retrieval.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAccountFieldValueType() throws Exception {
        final long probeId = 1123124;
        final String defaultValue = "Some default value";
        final ProbeInfo.Builder builder =
                populateProbeInfo(oneAccountFieldProbe, "type1", "category1",
                        Collections.emptyList());
        final AccountDefEntry entry2 = AccountDefEntry.newBuilder()
                .setCustomDefinition(
                        CustomAccountDefEntry.newBuilder(mandatoryEntry.getCustomDefinition())
                                .setPrimitiveValue(PrimitiveValue.BOOLEAN))
                .setDefaultValue(defaultValue)
                .build();
        final AccountDefEntry entry3 = AccountDefEntry.newBuilder()
                .setCustomDefinition(
                        CustomAccountDefEntry.newBuilder(mandatoryEntry.getCustomDefinition())
                                .setPrimitiveValue(PrimitiveValue.NUMERIC))
                .build();
        final AccountDefEntry entry4 = AccountDefEntry.newBuilder()
                .setCustomDefinition(
                        CustomAccountDefEntry.newBuilder(mandatoryEntry.getCustomDefinition())
                                .setGroupScope(GroupScopePropertySet.newBuilder()
                                        .setEntityType(EntityType.VIRTUAL_MACHINE)))
                .build();
        final AccountDefEntry entry5 = AccountDefEntry.newBuilder()
                .setPredefinedDefinition(PredefinedAccountDefinition.Username.name())
                .build();
        builder.addAccountDefinition(entry2);
        builder.addAccountDefinition(entry3);
        builder.addAccountDefinition(entry4);
        builder.addAccountDefinition(entry5);
        // builder.addAccountDefinition();
        Mockito.when(probeStore.getProbe(1123124)).thenReturn(Optional.of(builder.build()));
        final ProbeDescription resp = parseProbeDescription(probeId);
        Assert.assertEquals(builder.getProbeType(), resp.getType());
        final List<com.vmturbo.topology.processor.api.AccountDefEntry> entries =
                resp.getAccountDefinitions();
        Assert.assertEquals(AccountFieldValueType.STRING, entries.get(0).getValueType());
        Assert.assertEquals(AccountFieldValueType.BOOLEAN, entries.get(1).getValueType());
        Assert.assertEquals(AccountFieldValueType.NUMERIC, entries.get(2).getValueType());
        Assert.assertEquals(AccountFieldValueType.GROUP_SCOPE, entries.get(3).getValueType());
        Assert.assertEquals(AccountFieldValueType.STRING, entries.get(4).getValueType());

        Assert.assertNull(entries.get(0).getDefaultValue());
        Assert.assertEquals(defaultValue, entries.get(1).getDefaultValue());
        Assert.assertNull(entries.get(2).getDefaultValue());
        Assert.assertNull(entries.get(3).getDefaultValue());
        Assert.assertNull(entries.get(4).getDefaultValue());
    }

    /**
     * Tests that ProbeDescription which is returned in response has action policies.
     *
     * @throws Exception may be thrown in case of invalid probeId
     */
    @Test
    public void testProbeDescriptionHasXlActionPolicies() throws Exception {
        final long probeId = 1L;
        final List<ActionPolicyDTO> sdkActionPolicies =
                prepareProbeStoreForTestActionPolicies(probeId);
        final ProbeDescription probeDescription = parseProbeDescription(probeId);
        final List<ProbeActionCapability> responsePolicies = probeDescription.getActionPolicies();

        Assert.assertEquals(SdkToProbeActionsConverter.convert(sdkActionPolicies), responsePolicies);
    }

    private List<ActionPolicyDTO> prepareProbeStoreForTestActionPolicies(long probeId) {
        final ActionPolicyDTO changeActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.VIRTUAL_MACHINE,
                        ActionType.CHANGE);
        final ActionPolicyDTO startActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.PHYSICAL_MACHINE,
                        ActionType.START);
        final ActionPolicyDTO resizeActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.PHYSICAL_MACHINE,
                        ActionType.RIGHT_SIZE);
        final List<ActionPolicyDTO> sdkActionPolicies =
                Arrays.asList(changeActionPolicy, startActionPolicy, resizeActionPolicy);
        final ProbeInfo.Builder probeInfoBuilder =
                populateProbeInfo(null, "ProbeType", "Category", sdkActionPolicies);
        Mockito.when(probeStore.getProbe(probeId))
                .thenReturn(Optional.of(probeInfoBuilder.build()));
        return sdkActionPolicies;
    }

    /**
     * Parses ProbeDescription from response.
     *
     * @param probeId probe which description it parses
     * @return parsed description
     * @throws Exception may be thrown in case of invalid probeId
     */
    private ProbeDescription parseProbeDescription(long probeId) throws Exception {
        final String resultStr = requestProbe(probeId).andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        return GSON.fromJson(resultStr, ProbeDescription.class);
    }

    /**
     * Test getting probe, that is absent in the probe store.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testGetAbsentProbe() throws Exception {
        final long probeId = -1L;
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.empty());
        final MvcResult result = requestProbe(probeId).andExpect(status().isNotFound()).andReturn();
        final String resultStr = result.getResponse().getContentAsString();
        final ProbeDescription resp = GSON.fromJson(resultStr, ProbeDescription.class);
        Assert.assertThat(resp.getError(), CoreMatchers.containsString(Long.toString(probeId)));
        Assert.assertThat(resp.getError(), CoreMatchers.containsString("not found"));
    }

    /**
     * Populates ProbeInfo builder by certain data.
     *
     * @param probeType must be presented
     * @param category must be presented
     * @param actionPolicies policies to add to probeInfo. May be null of empty if not presented
     * @return populated builder
     */
    private static ProbeInfo.Builder populateProbeInfo(@Nullable ProbeInfo prototype,
            @Nonnull String probeType, @Nonnull String category,
            @Nonnull List<ActionPolicyDTO> actionPolicies) {
        ProbeInfo.Builder builder = Optional.ofNullable(prototype).map(ProbeInfo::newBuilder)
                .orElse(ProbeInfo.newBuilder());
        builder.setProbeType(probeType).setProbeCategory(category).setUiProbeCategory("uiProbeCat");
        actionPolicies.forEach(builder::addActionPolicy);
        return builder;
    }

    private ResultActions requestProbe(long probeId) throws Exception {
        return mockMvc.perform(
                get("/probe/" + probeId).accept(MediaType.APPLICATION_JSON_UTF8_VALUE));
    }

    private List<ProbeDescription> getAllProbes() throws Exception {
        final MvcResult result =
                mockMvc.perform(get("/probe").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(status().isOk())
                        .andReturn();
        final String resultStr = result.getResponse().getContentAsString();
        final GetAllProbes resp = GSON.fromJson(resultStr, GetAllProbes.class);
        return resp.getProbes();
    }

    private void checkAccountField(AccountDefEntry defEntry, AccountField accountField) {
        Assert.assertEquals(defEntry.getCustomDefinition().getName(), accountField.getName());
        Assert.assertEquals(defEntry.getCustomDefinition().getDisplayName(),
                accountField.getDisplayName());
        Assert.assertEquals(defEntry.getCustomDefinition().getDescription(),
                accountField.getDescription());
        Assert.assertEquals(defEntry.getMandatory(), accountField.isRequired());
        Assert.assertEquals(defEntry.getCustomDefinition().getPrimitiveValue().name(),
                accountField.getValueType().name());
    }

    private void checkProbeDescription(ProbeInfo info, ProbeDescription desc) {
        Assert.assertEquals(info.getProbeType(), desc.getType());
        Assert.assertEquals(info.getProbeCategory(), desc.getCategory());
        // Expect account fields to be in the same order
        Assert.assertEquals(info.getAccountDefinitionCount(), desc.getAccountFields().size());
        IntStream.range(0, info.getAccountDefinitionCount()).forEach(i -> {
            checkAccountField(info.getAccountDefinition(i), desc.getAccountFields().get(i));
        });
    }
}
