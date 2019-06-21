package com.vmturbo.action.orchestrator.action;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link ActionDescriptionBuilder}.
 */
public class ActionDescriptionBuilderTest {

    private ActionDTO.Action moveRecommendation;
    private ActionDTO.Action resizeRecommendation;
    private ActionDTO.Action deactivateRecommendation;
    private ActionDTO.Action activateRecommendation;
    private ActionDTO.Action reconfigureRecommendation;
    private ActionDTO.Action provisionRecommendation;
    private ActionDTO.Action deleteRecommendation;
    private ActionDTO.Action buyRIRecommendation;

    private final Long VM1_ID = 11L;
    private final String VM1_DISPLAY_NAME = "vm1_test";
    private final Long PM_SOURCE_ID = 22L;
    private final String PM_SOURCE_DISPLAY_NAME = "pm_source_test";
    private final Long PM_DESTINATION_ID = 33L;
    private final String PM_DESTINATION_DISPLAY_NAME = "pm_destination_test";
    private final Long ST_SOURCE_ID = 44L;
    private final String ST_SOURCE_DISPLAY_NAME = "storage_source_test";
    private final Long COMPUTE_TIER_ID = 100L;
    private final String COMPUTE_TIER_DISPLAY_NAME = "tier_t1";
    private final Long MASTER_ACCOUNT_ID = 101L;
    private final String MASTER_ACCOUNT_DISPLAY_NAME = "my.super.account";
    private final Long REGION_ID = 102L;
    private final String REGION_DISPLAY_NAME = "Manhattan";

    private EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        moveRecommendation =
                makeRec(makeMoveInfo(VM1_ID, PM_SOURCE_ID, EntityType.PHYSICAL_MACHINE.getNumber(),
                    PM_DESTINATION_ID, EntityType.PHYSICAL_MACHINE.getNumber()),
                            SupportLevel.SUPPORTED).build();
        resizeRecommendation = makeRec(makeResizeInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeRec(makeActivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        reconfigureRecommendation = makeRec(makeReconfigureInfo(VM1_ID, PM_SOURCE_ID),
            SupportLevel.SUPPORTED).build();
        provisionRecommendation = makeRec(makeProvisionInfo(ST_SOURCE_ID),
            SupportLevel.SUPPORTED).build();
        deleteRecommendation = makeRec(makeDeleteInfo(ST_SOURCE_ID),
            SupportLevel.SUPPORTED).build();
        buyRIRecommendation = makeRec(makeBuyRIInfo(COMPUTE_TIER_ID,MASTER_ACCOUNT_ID,REGION_ID),
            SupportLevel.SUPPORTED).build();
    }

    public static ActionDTO.Action.Builder makeRec(ActionInfo.Builder infoBuilder,
                                             final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setImportance(0)
                .setExecutable(true)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }

    private ActionInfo.Builder makeResizeInfo(long targetId) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(26).build())
                .setNewCapacity(20)
                .setOldCapacity(10)
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId)));
    }

    private ActionInfo.Builder makeDeactivateInfo(long targetId) {
        return ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(26).build()));
    }

    private ActionInfo.Builder makeActivateInfo(long targetId) {
        return ActionInfo.newBuilder().setActivate(Activate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(26).build()));
    }

    private ActionInfo.Builder makeReconfigureInfo(long targetId, long sourceId) {
        return ActionInfo.newBuilder().setReconfigure(Reconfigure.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .setSource(ActionOrchestratorTestUtils.createActionEntity(sourceId))
                .build());
    }

    private ActionInfo.Builder makeProvisionInfo(long targetId) {
        return ActionInfo.newBuilder().setProvision(Provision.newBuilder()
            .setEntityToClone(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.STORAGE_VALUE)
                .build())
            .build());
    }

    private ActionInfo.Builder makeDeleteInfo(long targetId) {
        return ActionInfo.newBuilder().setDelete(Delete.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.STORAGE_VALUE)
                .build())
            .setFilePath("/file/to/delete/filename.test")
            .build());
    }

    private ActionInfo.Builder makeBuyRIInfo(long computeTier, long masterAccount, long region) {
        return ActionInfo.newBuilder().setBuyRi(BuyRI.newBuilder()
            .setComputeTier(ActionEntity.newBuilder()
                .setId(computeTier)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build())
            .setMasterAccount(ActionEntity.newBuilder()
                .setId(masterAccount)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build())
            .setRegionId(ActionEntity.newBuilder()
                .setId(region)
                .setType(EntityType.REGION_VALUE)
                .build())
            .build());
    }

    private ActionInfo.Builder makeMoveInfo(
        long targetId,
        long sourceId,
        int sourceType,
        long destinationId,
        int destinationType) {

        return ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
            .addChanges(ChangeProvider.newBuilder()
                .setSource(ActionEntity.newBuilder()
                    .setId(sourceId)
                    .setType(sourceType)
                    .build())
                .setDestination(ActionEntity.newBuilder()
                    .setId(destinationId)
                    .setType(destinationType)
                    .build())
                .build())
            .build());
    }

    public static Optional<ActionPartialEntity> createEntity(Long Oid, int entityType,
                                                             String displayName) {
        return Optional.of(ActionPartialEntity.newBuilder()
            .setOid(Oid)
            .setEntityType(entityType)
            .setDisplayName(displayName)
            .build());
    }

    @Test
    public void testBuildMoveActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn((createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_DESTINATION_ID)))
            .thenReturn((createEntity(PM_DESTINATION_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_DESTINATION_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, moveRecommendation);
        Assert.assertEquals(description, "Move Virtual Machine vm1_test from pm_source_test to pm_destination_test");
    }

    @Test
    public void testBuildReconfigureActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn((createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, reconfigureRecommendation);
        Assert.assertTrue(description.contains("Reconfigure Virtual Machine vm1_test"));
    }

    @Test
    public void testBuildResizeActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeRecommendation);

        Assert.assertEquals(description, "Resize up Vcpu for Virtual Machine vm1_test from 10 to 20");
    }

    @Test
    public void testBuildDeactivateActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deactivateRecommendation);

        Assert.assertEquals(description, "Suspend Virtual Machine vm1_test");
    }

    @Test
    public void testBuildActivateActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, activateRecommendation);

        Assert.assertEquals(description, "Start Virtual Machine vm1_test due to increased demand for resources");
    }

    @Test
    public void testBuildProvisionActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE.getNumber(),
                ST_SOURCE_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, provisionRecommendation);

        Assert.assertEquals(description, "Provision Storage storage_source_test");
    }

    @Test
    public void testBuildDeleteActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE.getNumber(),
                ST_SOURCE_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteRecommendation);

        Assert.assertEquals(description, "Delete wasted file 'filename.test' from Storage storage_source_test to free up 0 bytes");
    }

    @Test
    public void testBuildBuyRIActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_ID)))
            .thenReturn((createEntity(COMPUTE_TIER_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_DISPLAY_NAME)));
        when(entitySettingsCache.getEntityFromOid(eq(MASTER_ACCOUNT_ID)))
            .thenReturn((createEntity(MASTER_ACCOUNT_ID,
                EntityType.BUSINESS_ACCOUNT.getNumber(),
                MASTER_ACCOUNT_DISPLAY_NAME)));
        when(entitySettingsCache.getEntityFromOid(eq(REGION_ID)))
            .thenReturn((createEntity(REGION_ID,
                EntityType.REGION.getNumber(),
                REGION_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, buyRIRecommendation);

        Assert.assertEquals(description, "Buy 0 tier_t1 RIs for my.super.account in Manhattan");
    }
}
