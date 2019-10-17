package com.vmturbo.action.orchestrator.action;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link ActionDescriptionBuilder}.
 */
public class ActionDescriptionBuilderTest {

    private ActionDTO.Action moveRecommendation;
    private ActionDTO.Action scaleRecommendation;
    private ActionDTO.Action resizeRecommendation;
    private ActionDTO.Action resizeMemRecommendation;
    private ActionDTO.Action resizeMemReservationRecommendation;
    private ActionDTO.Action deactivateRecommendation;
    private ActionDTO.Action activateRecommendation;
    private ActionDTO.Action reconfigureRecommendation;
    private ActionDTO.Action provisionBySupplyRecommendation;
    private ActionDTO.Action provisionByDemandRecommendation;
    private ActionDTO.Action deleteRecommendation;
    private ActionDTO.Action deleteCloudStorageRecommendation;
    private ActionDTO.Action deleteCloudStorageRecommendationWithNoSourceEntity;
    private ActionDTO.Action buyRIRecommendation;

    private final Long VM1_ID = 11L;
    private final String VM1_DISPLAY_NAME = "vm1_test";
    private final Long PM_SOURCE_ID = 22L;
    private final String PM_SOURCE_DISPLAY_NAME = "pm_source_test";
    private final Long PM_DESTINATION_ID = 33L;
    private final String PM_DESTINATION_DISPLAY_NAME = "pm_destination_test";
    private final Long ST_SOURCE_ID = 44L;
    private final String ST_SOURCE_DISPLAY_NAME = "storage_source_test";
    private final Long ST_DESTINATION_ID = 55L;
    private final String ST_DESTINATION_DISPLAY_NAME = "storage_destination_test";
    private final Long VV_ID = 66L;
    private final String VV_DISPLAY_NAME = "volume_display_name";
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
        scaleRecommendation =
                makeRec(makeMoveInfo(VM1_ID, ST_SOURCE_ID, EntityType.STORAGE_TIER.getNumber(),
                        ST_DESTINATION_ID, EntityType.STORAGE_TIER.getNumber()),
                        SupportLevel.SUPPORTED).build();
        resizeRecommendation = makeRec(makeResizeInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        resizeMemRecommendation = makeRec(makeResizeMemInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        resizeMemReservationRecommendation =
                makeRec(makeResizeReservationMemInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeRec(makeActivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        reconfigureRecommendation = makeRec(makeReconfigureInfo(VM1_ID, PM_SOURCE_ID),
            SupportLevel.SUPPORTED).build();
        Explanation explanation1 = Explanation.newBuilder()
            .setProvision(makeProvisionBySupplyExplanation()).build();
        provisionBySupplyRecommendation = makeRec(makeProvisionInfo(ST_SOURCE_ID),
            SupportLevel.SUPPORTED, explanation1).build();
        Explanation explanation2 = Explanation.newBuilder()
            .setProvision(makeProvisionByDemandExplanation()).build();
        provisionByDemandRecommendation = makeRec(makeProvisionInfo(PM_SOURCE_ID),
            SupportLevel.SUPPORTED, explanation2).build();
        deleteRecommendation = makeRec(makeDeleteInfo(ST_SOURCE_ID),
            SupportLevel.SUPPORTED).build();
        deleteCloudStorageRecommendation = makeRec(makeDeleteCloudStorageInfo(VV_ID, Optional.of(ST_SOURCE_ID)),
            SupportLevel.SUPPORTED).build();
        deleteCloudStorageRecommendationWithNoSourceEntity = makeRec(makeDeleteCloudStorageInfo(VV_ID, Optional.empty()),
            SupportLevel.SUPPORTED).build();
        buyRIRecommendation = makeRec(makeBuyRIInfo(COMPUTE_TIER_ID,MASTER_ACCOUNT_ID,REGION_ID),
            SupportLevel.SUPPORTED).build();
    }

    /**
     * Build an {@link ActionDTO.Action}.
     *
     * @param infoBuilder {@link ActionInfo.Builder}
     * @param supportLevel {@link SupportLevel}
     * @return {@link ActionDTO.Action.Builder}
     */
    private static ActionDTO.Action.Builder makeRec(final ActionInfo.Builder infoBuilder,
                                                    final SupportLevel supportLevel) {
        return makeRec(infoBuilder, supportLevel, Explanation.newBuilder().build());
    }

    /**
     * Build an {@link ActionDTO.Action}.
     *
     * @param infoBuilder {@link ActionInfo.Builder}
     * @param supportLevel {@link SupportLevel}
     * @param explanation {@link Explanation}
     * @return {@link ActionDTO.Action.Builder}
     */
    private static ActionDTO.Action.Builder makeRec(final ActionInfo.Builder infoBuilder,
                                                    final SupportLevel supportLevel,
                                                    final Explanation explanation) {
        return ActionDTO.Action.newBuilder()
            .setId(IdentityGenerator.next())
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setSupportingLevel(supportLevel)
            .setInfo(infoBuilder).setExplanation(explanation);
    }

    /**
     * Create a resize action info.
     *
     * @param targetId the target entity id
     * @param commodityType type of the commodity to resize
     * @param oldCapacity old capacity
     * @param newCapacity new capacity
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeInfo(long targetId, int commodityType, float oldCapacity,
            float newCapacity) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType).build())
                .setOldCapacity(oldCapacity)
                .setNewCapacity(newCapacity)
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId)));
    }

    /**
     * Create a resize action for vcpu commodity.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeInfo(long targetId) {
        return makeResizeInfo(targetId, 26, 10, 20);
    }

    /**
     * Create a resize action for mem commodity.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeMemInfo(long targetId) {
        return makeResizeInfo(targetId, 21, 16 * Units.MBYTE, 8 * Units.MBYTE);
    }

    /**
     * Create a resize action for mem commodity and on reserved attribute.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeReservationMemInfo(long targetId) {
        ActionInfo.Builder builder = makeResizeInfo(targetId, 21, 16 * Units.MBYTE, 8 * Units.MBYTE);
        builder.getResizeBuilder().setCommodityAttribute(CommodityAttribute.RESERVED);
        return builder;
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

    private ActionInfo.Builder makeDeleteCloudStorageInfo(long targetId, Optional<Long> sourceIdOpt) {
        Delete.Builder deleteBuilder = Delete.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());

        if (sourceIdOpt.isPresent()) {
            deleteBuilder.setSource(ActionEntity.newBuilder()
                .setId(sourceIdOpt.get())
                .setType(EntityType.STORAGE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());
        }

        return ActionInfo.newBuilder().setDelete(deleteBuilder.build());
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
            .setRegion(ActionEntity.newBuilder()
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

    /**
     * Create a {@link ProvisionBySupplyExplanation}.
     *
     * @return {@link ProvisionExplanation}
     */
    private ProvisionExplanation makeProvisionBySupplyExplanation() {
        return ProvisionExplanation.newBuilder()
            .setProvisionBySupplyExplanation(
                ProvisionBySupplyExplanation.newBuilder()
                    .setMostExpensiveCommodityInfo(ReasonCommodity.newBuilder()
                        .setCommodityType(CommodityType.newBuilder().setType(22)))
                    .build())
            .build();
    }

    /**
     * Create a {@link ProvisionByDemandExplanation}
     *
     * @return {@link ProvisionExplanation}
     */
    private ProvisionExplanation makeProvisionByDemandExplanation() {
        List<CommodityNewCapacityEntry> capacityPerType = new ArrayList<>();
        capacityPerType.add(ProvisionByDemandExplanation.CommodityNewCapacityEntry.newBuilder()
            .setCommodityBaseType(CommodityDTO.CommodityType.MEM_VALUE)
            .setNewCapacity(10).build());
        return ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(ProvisionByDemandExplanation
                .newBuilder().setBuyerId(VM1_ID)
                .addAllCommodityNewCapacityEntry(capacityPerType)
                .addAllCommodityMaxAmountAvailable(new ArrayList<>()).build())
            .build();
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
    public void testBuildScaleActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
                .thenReturn((createEntity(ST_SOURCE_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_SOURCE_DISPLAY_NAME)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_DESTINATION_ID)))
                .thenReturn((createEntity(ST_DESTINATION_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_DESTINATION_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, scaleRecommendation);
        Assert.assertEquals(description,
            "Scale Virtual Machine vm1_test from storage_source_test to storage_destination_test");
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

        Assert.assertEquals(description, "Resize up VCPU for Virtual Machine vm1_test from 10 to 20");
    }

    @Test
    public void testBuildResizeMemActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeMemRecommendation);

        Assert.assertEquals(description, "Resize down Mem for Virtual Machine vm1_test from 16 GB to 8 GB");
    }

    /**
     * Test resize reservation action description.
     */
    @Test
    public void testBuildResizeMemReservationActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME)));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeMemReservationRecommendation);

        Assert.assertEquals(description,
                "Resize down Mem reservation for Virtual Machine vm1_test from 16 GB to 8 GB");
    }

    /**
     * Test that the unit is converted to more readable format in the final description for
     * resize DBMem action.
     */
    @Test
    public void testBuildResizeDBMemActionDescription() {
        final long dbsId = 19L;
        when(entitySettingsCache.getEntityFromOid(eq(dbsId))).thenReturn(
                createEntity(dbsId, EntityType.DATABASE_SERVER_VALUE, "sqlServer1"));
        ActionDTO.Action resizeDBMem = makeRec(makeResizeInfo(dbsId,
                CommodityDTO.CommodityType.DB_MEM_VALUE, 1622744.0f, 1445841.0f),
                SupportLevel.SUPPORTED).build();
        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeDBMem);
        Assert.assertEquals(description,
                "Resize down DB Mem for Database Server sqlServer1 from 1.548 GB to 1.379 GB");
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

    /**
     * Test ProvisionBySupply action description.
     */
    @Test
    public void testBuildProvisionBySupplyActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn(createEntity(ST_SOURCE_ID,
                EntityType.STORAGE.getNumber(),
                ST_SOURCE_DISPLAY_NAME));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, provisionBySupplyRecommendation);

        Assert.assertEquals(description, "Provision Storage storage_source_test");
    }

    /**
     * Test ProvisionByDemand action description.
     */
    @Test
    public void testBuildProvisionByDemandActionDescription() {
        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn(createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME));
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn(createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, provisionByDemandRecommendation);

        Assert.assertEquals(description,
            "Provision Physical Machine similar to pm_source_test with scaled up Mem due to vm1_test");
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
    public void testBuildDeleteCloudStorageActionDescription_whenSearchServiceHasNoBAInfo() {
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE_TIER.getNumber(),
                ST_SOURCE_DISPLAY_NAME)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        Assert.assertEquals(description, "Delete Unattached storage_source_test Volume volume_display_name");
    }

    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenSearchServiceHasBAInfo() {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE_TIER.getNumber(),
                ST_SOURCE_DISPLAY_NAME)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(TopologyEntityDTO.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        Assert.assertEquals(description, "Delete Unattached storage_source_test Volume volume_display_name from business_account_name");
    }

    /**
     * Test Delete Cloud Storage Action when snapshot has no BA info.
     */
    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenSnapshotDoesNotHaveSourceEntity_SearchServiceHasBAInfo() {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn(Optional.empty());
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(TopologyEntityDTO.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        Assert.assertEquals(description, "Delete Unattached  Volume volume_display_name from business_account_name");
    }

    /**
     * Test Delete Cloud Storage Action Description when action has no source entity.
     */
    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenActionHasNoSourceEntity_SearchServiceHasBAInfo() {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(TopologyEntityDTO.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendationWithNoSourceEntity);

        Assert.assertEquals(description, "Delete Unattached  Volume volume_display_name from business_account_name");
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