package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test for {@link DataExtractionPendingActionWriter}.
 */
public class DataExtractionPendingActionWriterTest {

    private static final Logger logger = LogManager.getLogger();

    private static final MultiStageTimer timer = new MultiStageTimer(logger);

    private static final long vm1 = 10L;
    private static final long host1 = 20L;
    private static final long storage1 = 30L;

    private static final ActionOrchestratorAction ACTION = ActionOrchestratorAction.newBuilder()
            .setActionId(1001L)
            .setActionSpec(ActionSpec.newBuilder()
                    .setActionState(ActionState.READY)
                    .setActionMode(ActionMode.MANUAL)
                    .setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .setSeverity(Severity.MAJOR)
                    .setDescription("Move vm1 from host1 to host2")
                    .setRecommendation(ActionDTO.Action.newBuilder()
                            .setId(1001L)
                            .setInfo(ActionInfo.newBuilder()
                                    .setMove(Move.newBuilder()
                                            .setTarget(ActionEntity.newBuilder()
                                                    .setId(vm1)
                                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                            .setExecutable(true)
                            .setSupportingLevel(SupportLevel.SUPPORTED)
                            .setDeprecatedImportance(0)
                            .setExplanation(Explanation.getDefaultInstance())))
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);

    private DataExtractionFactory dataExtractionFactory = mock(DataExtractionFactory.class);

    private RelatedEntitiesExtractor relatedEntitiesExtractor = mock(RelatedEntitiesExtractor.class);

    private DataProvider dataProvider = mock(DataProvider.class);
    private final SupplyChain supplyChain = mock(SupplyChain.class);
    private final TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private MutableLong lastWrite = new MutableLong(0);

    private ActionConverter actionConverter;

    private DataExtractionPendingActionWriter writer;

    private List<ExportedObject> actionsCapture;

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        // capture actions sent to kafka
        this.actionsCapture = new ArrayList<>();
        doAnswer(inv -> {
            Collection<ExportedObject> exportedObjects = inv.getArgumentAt(0, Collection.class);
            if (exportedObjects != null) {
                actionsCapture.addAll(exportedObjects);
            }
            return null;
        }).when(extractorKafkaSender).send(any());

        mockEntity(vm1, EntityType.VIRTUAL_MACHINE_VALUE);

        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        when(dataProvider.getSupplyChain()).thenReturn(supplyChain);

        doReturn(Optional.of(relatedEntitiesExtractor)).when(dataExtractionFactory).newRelatedEntitiesExtractor(any());

        RelatedEntity relatedEntity1 = new RelatedEntity();
        relatedEntity1.setOid(host1);
        relatedEntity1.setName(String.valueOf(host1));
        RelatedEntity relatedEntity2 = new RelatedEntity();
        relatedEntity2.setOid(storage1);
        relatedEntity2.setName(String.valueOf(storage1));
        doReturn(ImmutableMap.of(
                PHYSICAL_MACHINE.getLiteral(), Lists.newArrayList(relatedEntity1),
                STORAGE.getLiteral(), Lists.newArrayList(relatedEntity2)
        )).when(relatedEntitiesExtractor).extractRelatedEntities(vm1);

        actionConverter = spy(new ActionConverter(
                new ActionAttributeExtractor(), mock(ObjectMapper.class)));
        writer = spy(new DataExtractionPendingActionWriter(extractorKafkaSender, dataExtractionFactory,
                dataProvider, clock, lastWrite, actionConverter));
    }

    /**
     * Test that compound move action is extracted correctly. The action contains 4 providers
     * changes: 1 host move + 3 storage moves.
     */
    @Test
    public void testExtractAction() {
        // extract
        writer.recordAction(ACTION);
        writer.write(timer);

        // verify
        assertThat(actionsCapture.size(), is(1));

        final ExportedObject obj = actionsCapture.get(0);
        final Action action = obj.getAction();
        // common fields
        verifyCommonFields(obj, ACTION.getActionSpec());
        // target
        assertThat(action.getTarget().getOid(), is(vm1));
        assertThat(action.getTarget().getName(), is(String.valueOf(vm1)));
        assertThat(action.getTarget().getType(), is(VIRTUAL_MACHINE.getLiteral()));
        // related
        assertThat(action.getRelated().size(), is(2));
        assertThat(action.getRelated().get(PHYSICAL_MACHINE.getLiteral()).get(0).getOid(), is(host1));
        assertThat(action.getRelated().get(STORAGE.getLiteral()).get(0).getOid(), is(storage1));

        verify(actionConverter).makeExportedAction(ACTION.getActionSpec(), topologyGraph,
                new HashMap<>(), Optional.of(relatedEntitiesExtractor));
    }

    /**
     * Verify the common fields in {@link Action}.
     *
     * @param exportedObject action sent to Kafka
     * @param actionSpec action from AO
     */
    private void verifyCommonFields(ExportedObject exportedObject, ActionSpec actionSpec) {
        assertThat(exportedObject.getTimestamp(), is(ExportUtils.getFormattedDate(clock.millis())));
        final Action action = exportedObject.getAction();
        assertThat(action.getCreationTime(), is(ExportUtils.getFormattedDate(actionSpec.getRecommendationTime())));
        assertThat(action.getOid(), is(actionSpec.getRecommendation().getId()));
        assertThat(action.getState(), is(actionSpec.getActionState().name()));
        assertThat(action.getCategory(), is(actionSpec.getCategory().name()));
        assertThat(action.getMode(), is(actionSpec.getActionMode().name()));
        assertThat(action.getSeverity(), is(actionSpec.getSeverity().name()));
        assertThat(action.getDescription(), is(actionSpec.getDescription()));
        assertThat(action.getType(), is(ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation()).name()));
    }

    /**
     * Mock a {@link SupplyChainEntity}.
     *
     * @param entityId   Given entity ID.
     * @param entityType Given entity Type.
     */
    private void mockEntity(long entityId, int entityType) {
        SupplyChainEntity entity = mock(SupplyChainEntity.class);
        when(entity.getOid()).thenReturn(entityId);
        when(entity.getEntityType()).thenReturn(entityType);
        when(entity.getDisplayName()).thenReturn(String.valueOf(entityId));
        doReturn(Optional.of(entity)).when(topologyGraph).getEntity(entityId);
    }
}