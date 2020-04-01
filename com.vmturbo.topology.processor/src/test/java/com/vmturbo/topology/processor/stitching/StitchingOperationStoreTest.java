package com.vmturbo.topology.processor.stitching;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.ListStringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary.StitchingUnknownProbeException;
import com.vmturbo.stitching.StringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringToStringDataDrivenStitchingOperation;
import com.vmturbo.topology.processor.probes.ProbeException;

public class StitchingOperationStoreTest {

    final StitchingOperationLibrary library = Mockito.mock(StitchingOperationLibrary.class);

    final StitchingOperationStore store = new StitchingOperationStore(library);

    final StitchingOperation<?, ?> firstOperation = mock(StitchingOperation.class);
    final StitchingOperation<?, ?> secondOperation = mock(StitchingOperation.class);
    final StitchingOperation<?, ?> thirdOperation = mock(StitchingOperation.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddOperationsViaLibrary() throws Exception {
        when(library.stitchingOperationsFor(eq("some-hypervisor-probe"), eq(ProbeCategory.HYPERVISOR)))
            .thenReturn(Collections.singletonList(firstOperation));
        final MediationMessage.ProbeInfo probeInfo = MediationMessage.ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setUiProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setProbeType("some-hypervisor-probe")
            .build();

        store.setOperationsForProbe(1234L, probeInfo, Sets.newHashSet());

        assertEquals(1, store.probeCount());
        assertEquals(Collections.singletonList(firstOperation), store.getOperationsForProbe(1234L).get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAddUnknownProbe() throws StitchingUnknownProbeException, ProbeException {
        when(library.stitchingOperationsFor(eq("unknown-probe"), eq(ProbeCategory.HYPERVISOR)))
            .thenThrow(StitchingUnknownProbeException.class);

        final MediationMessage.ProbeInfo probeInfo = MediationMessage.ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setUiProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setProbeType("unknown-probe")
            .build();

        expectedException.expect(ProbeException.class);
        store.setOperationsForProbe(1234L, probeInfo, Sets.newHashSet());
    }

    @Test
    public void testGetAllOperations() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertThat(store.getAllOperations().stream()
            .map(pso -> pso.stitchingOperation)
            .collect(Collectors.toList()), containsInAnyOrder(firstOperation, secondOperation, thirdOperation));
    }

    @Test
    public void testProbeCount() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertEquals(2, store.probeCount());
    }

    @Test
    public void testOperationCount() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertEquals(2, store.probeCount());
    }

    @Test
    public void testRemoveOperationsForProbe() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));
        assertEquals(3, store.operationCount());

        store.removeOperationsForProbe(1234);
        assertEquals(1, store.operationCount());
        assertEquals(Optional.<List<StitchingOperation>>empty(), store.getOperationsForProbe(1234));
    }

    private TemplateDTO createTemplateDTO(ReturnType internalMatchingType,
                                          ReturnType externalMatchingType,
                                          EntityType templateClass) {
        EntityField externalNames = EntityField.newBuilder().addMessagePath("storage_data")
                .setFieldName("externalName").build();
        MatchingData matchingData = MatchingData.newBuilder()
                .setMatchingField(externalNames).build();
        MatchingMetadata matchingMetadata = MatchingMetadata.newBuilder()
                .addMatchingData(matchingData).setReturnType(internalMatchingType)
                .addExternalEntityMatchingProperty(matchingData)
                .setExternalEntityReturnType(externalMatchingType).build();
        final MergedEntityMetadata storageMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(matchingMetadata)
                        .build();
        final TemplateDTO templateDTO = TemplateDTO.newBuilder()
                .setTemplateClass(templateClass)
                .setTemplateType(TemplateType.BASE)
                .setTemplatePriority(1)
                .setMergedEntityMetaData(storageMergeEntityMetadata).build();
        return templateDTO;
    }


    private MediationMessage.ProbeInfo createProbeInfo(String probeType,
                                                      TemplateDTO... templateDTOs) {
        List<TemplateDTO> templateDTOList = Lists.newArrayList(templateDTOs);
        final MediationMessage.ProbeInfo probeInfo = MediationMessage.ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.STORAGE.name())
                .setUiProbeCategory(ProbeCategory.STORAGE.name())
                .setProbeType(probeType)
                .addAllSupplyChainDefinitionSet(templateDTOList)
                .build();
        return probeInfo;
    }

    @Test
    public void testCreateOperationFromProbeInfo() throws ProbeException {
        final MediationMessage.ProbeInfo probe1Info = createProbeInfo("storage-probe-1",
                createTemplateDTO(ReturnType.LIST_STRING, ReturnType.LIST_STRING,
                        EntityType.STORAGE));
        final MediationMessage.ProbeInfo probe2Info = createProbeInfo("storage-probe-2",
                createTemplateDTO(ReturnType.LIST_STRING, ReturnType.STRING,
                        EntityType.STORAGE));
        final MediationMessage.ProbeInfo probe3Info = createProbeInfo("storage-probe-3",
                createTemplateDTO(ReturnType.STRING, ReturnType.LIST_STRING,
                        EntityType.STORAGE));
        final MediationMessage.ProbeInfo probe4Info = createProbeInfo("storage-probe-4",
                createTemplateDTO(ReturnType.STRING, ReturnType.STRING,
                        EntityType.STORAGE));

        store.setOperationsForProbe(4321, probe1Info, Sets.newHashSet());
        store.setOperationsForProbe(5432, probe2Info, Sets.newHashSet());
        store.setOperationsForProbe(6543, probe3Info, Sets.newHashSet());
        store.setOperationsForProbe(7654, probe4Info, Sets.newHashSet());
        assertTrue(store.getOperationsForProbe(4321).get().get(0)
                instanceof ListStringToListStringDataDrivenStitchingOperation);
        assertTrue(store.getOperationsForProbe(5432).get().get(0)
                instanceof ListStringToStringDataDrivenStitchingOperation);
        assertTrue(store.getOperationsForProbe(6543).get().get(0)
                instanceof StringToListStringDataDrivenStitchingOperation);
        assertTrue(store.getOperationsForProbe(7654).get().get(0)
                instanceof StringToStringDataDrivenStitchingOperation);
    }

    @Test
    public void testCreateMultipleOperationsFromProbeInfo() throws ProbeException {
        final MediationMessage.ProbeInfo probe1Info = createProbeInfo("storage-probe-10",
                createTemplateDTO(ReturnType.LIST_STRING, ReturnType.LIST_STRING,
                        EntityType.STORAGE),
                createTemplateDTO(ReturnType.STRING, ReturnType.STRING,
                        EntityType.DISK_ARRAY));
        store.setOperationsForProbe(2468, probe1Info, Sets.newHashSet());
        List<StitchingOperation<?, ?>> stitchingOperations =
                store.getOperationsForProbe(2468).get();
        assertEquals(2, stitchingOperations.size());
        assertTrue(stitchingOperations.get(0)
                instanceof ListStringToListStringDataDrivenStitchingOperation);
        assertTrue(stitchingOperations.get(1)
                instanceof StringToStringDataDrivenStitchingOperation);
        assertEquals(EntityType.STORAGE, stitchingOperations.get(0).getInternalEntityType());
        assertEquals(EntityType.DISK_ARRAY, stitchingOperations.get(1).getInternalEntityType());
    }

    /**
     * tests the combination of data driven and custom stitching operations for a probe where
     * there is no overlap of entity types between the two.
     * @throws Exception exceptions thrown by the stitching library
     */
    @Test
    public void testDataAndCustomOperationsWithNoOverlap() throws Exception {

        final long probeId = 2468L;
        final String customOp = "custom";
        // set up custom operation
        when(library.stitchingOperationsFor(eq("some-hypervisor-probe"), eq(ProbeCategory.STORAGE)))
                .thenReturn(Collections.singletonList(firstOperation));
        when(firstOperation.getOperationName()).thenReturn(customOp);
        when(firstOperation.getInternalEntityType()).thenReturn(EntityType.BUSINESS_ACCOUNT);
        when(firstOperation.getExternalEntityType()).thenReturn(Optional.of(EntityType.BUSINESS_ACCOUNT));

        // set up data driven operation
        final MediationMessage.ProbeInfo probe1Info = createProbeInfo("some-hypervisor-probe",
                createTemplateDTO(ReturnType.LIST_STRING, ReturnType.LIST_STRING,
                        EntityType.SERVICE_PROVIDER));
        store.setOperationsForProbe(probeId, probe1Info, Sets.newHashSet());
        assertEquals(2, store.getOperationsForProbe(probeId).get().size());
        String opName1 = store.getOperationsForProbe((probeId)).get().get(0).getOperationName();
        String opName2 = store.getOperationsForProbe((probeId)).get().get(1).getOperationName();
        assertEquals("ListStringToListStringDataDrivenStitchingOperation", opName1);
        assertEquals(customOp, opName2);

    }

    /**
     * tests the combination of data driven and custom stitching operations for a probe where
     * there is overlap of entity types between the two.
     * @throws Exception exceptions thrown by the stitching library
     */
    @Test
    public void testDataAndCustomOperationsWithOverlap() throws Exception {

        final long probeId = 2468L;
        // set up custom operation
        when(library.stitchingOperationsFor(eq("some-hypervisor-probe"), eq(ProbeCategory.STORAGE)))
                .thenReturn(Collections.singletonList(firstOperation));
        when(firstOperation.getInternalEntityType()).thenReturn(EntityType.BUSINESS_ACCOUNT);
        when(firstOperation.getExternalEntityType()).thenReturn(Optional.of(EntityType.BUSINESS_ACCOUNT));

        // set up data driven operation
        final MediationMessage.ProbeInfo probe1Info = createProbeInfo("some-hypervisor-probe",
                createTemplateDTO(ReturnType.LIST_STRING, ReturnType.LIST_STRING,
                        EntityType.BUSINESS_ACCOUNT));
        store.setOperationsForProbe(probeId, probe1Info, Sets.newHashSet());
        assertEquals(1, store.getOperationsForProbe(probeId).get().size());
        String opName = store.getOperationsForProbe((probeId)).get().get(0).getOperationName();
        assertEquals("ListStringToListStringDataDrivenStitchingOperation", opName);

    }

}