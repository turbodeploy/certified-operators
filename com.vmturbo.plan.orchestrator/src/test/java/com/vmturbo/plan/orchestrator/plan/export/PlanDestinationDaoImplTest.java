package com.vmturbo.plan.orchestrator.plan.export;

import static com.vmturbo.plan.orchestrator.plan.export.PlanDestinationDaoImpl.GSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.plan.PlanExportDTO;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanDestination;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImplTest.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit test for {@link PlanDestinationDaoImpl}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestPlanOrchestratorDBEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class PlanDestinationDaoImplTest {

    @Autowired(required = false)
    private TestPlanOrchestratorDBEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("tp");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private PlanDestinationDaoImpl planDestinationDaoImpl;

    /**
     * Setup for the test.
     *
     * @throws Exception any exception on setup.
     */
    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);

        DSLContext dsl;

        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.planEndpoint());
            dsl = dbEndpointConfig.planEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }

        planDestinationDaoImpl = new PlanDestinationDaoImpl(dsl, new IdentityInitializer(0));

    }

    /**
     * Test toPlanDestinationDTO() method.
     */
    @Test
    public void testToPlanDestinationDTO() {
        final long oId = 1234567L;
        final String externalId = "externalId";
        final PlanExportState planExportState = PlanExportState.NONE;
        final int planExportProgress = 0;
        final long marketId = 777777L;
        final long targetId = 23234238947L;
        final String displayName = "planDestination";
        final PlanExportStatus planExportStatus = PlanExportStatus.newBuilder().setProgress(planExportProgress).setState(planExportState).build();
        LocalDateTime now = LocalDateTime.now();

        final PlanExportDTO.PlanDestination planDestinationDTO = PlanExportDTO.PlanDestination.newBuilder()
            .setOid(oId)
            .setExternalId(externalId)
            .setDisplayName(displayName)
            .setTargetId(targetId)
            .setStatus(planExportStatus)
            .setMarketId(marketId)
            .build();

        PlanDestination planDestination = new PlanDestination();
        planDestination.setId(oId);
        planDestination.setExternalId(externalId);
        planDestination.setPlanDestination(planDestinationDTO);
        planDestination.setStatus(planExportStatus.getState().toString());
        planDestination.setDiscoveryTime(now);

        PlanExportDTO.PlanDestination result = planDestinationDaoImpl.toPlanDestinationDTO(planDestination);
        assertEquals(planDestinationDTO.getOid(), result.getOid());
        assertEquals(planDestinationDTO.getExternalId(), result.getExternalId());
        assertEquals(planDestinationDTO.getDisplayName(), result.getDisplayName());
        assertEquals(planDestinationDTO.getStatus(), result.getStatus());
        assertEquals(planDestinationDTO.getTargetId(), result.getTargetId());
        assertEquals(planDestinationDTO.getMarketId(), result.getMarketId());
    }

    /**
     * Test restoreFromDiags() method. Preexist non-in-progress record removed.  new record inserted.
     *
     * @throws IntegrityException any DB exception
     */
    @Test
    public void testRestoreFromDiagsSimple() throws IntegrityException {
        final long azureTargetId = 1233245L;
        final long marketID = 77777L;
        final PlanExportStatus.Builder planExportStatusNone = PlanExportStatus.newBuilder()
            .setState(PlanExportState.NONE)
            .setProgress(0);

        PlanExportDTO.PlanDestination planDestination = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("new plan destination A")
            .setExternalId("new externalId")
            .setMarketId(marketID)
            .setTargetId(azureTargetId)
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();
        final List<String> diags = Arrays.asList(
            GSON.toJson(planDestination)
        );
        try {
            planDestinationDaoImpl.restoreDiags(diags, null);
        } catch (DiagnosticsException e) {
            fail();
        }

        final List<PlanExportDTO.PlanDestination> results = planDestinationDaoImpl.getAllPlanDestinations();

        assertEquals(1, results.size());
        PlanExportDTO.PlanDestination result = results.get(0);
        assertEquals("externalId should be the same", planDestination.getExternalId(), result.getExternalId());
        assertEquals("oid should be the same", planDestination.getOid(), result.getOid());
    }

    /**
     * Test restoreFromDiags() method.
     *
     * @throws IntegrityException any DB exception
     */
    @Test
    public void testRestoreFromDiags() throws IntegrityException {
        final long azureTargetId = 1233245L;
        final long marketID = 77777L;
        final long planDestinationId = 8888888L;
        final PlanExportStatus.Builder planExportStatusNone = PlanExportStatus.newBuilder()
            .setState(PlanExportState.NONE)
            .setProgress(0);
        final PlanExportStatus.Builder planExportStatusInProgress = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setProgress(30);
        final String existingInProgressExternalId = "in progress external id";
        final String existingNoneExternalId = "none external id";

        final PlanExportDTO.PlanDestination preexistingInProgress = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("preexisting in-process")
            .setStatus(planExportStatusInProgress)
            .setExternalId(existingInProgressExternalId)
            .setTargetId(azureTargetId)
            .setMarketId(marketID)
            .build();
        planDestinationDaoImpl.createPlanDestination(preexistingInProgress);

        final PlanExportDTO.PlanDestination preexistingNone = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("preexisting none")
            .setStatus(planExportStatusNone)
            .setExternalId(existingNoneExternalId)
            .setTargetId(azureTargetId)
            .setMarketId(marketID)
            .build();
        planDestinationDaoImpl.createPlanDestination(preexistingNone);

        final String newRecordExternalId = "new externalId";
        PlanExportDTO.PlanDestination planDestination = PlanExportDTO.PlanDestination.newBuilder()
            .setOid(planDestinationId)
            .setDisplayName("new plan destination A")
            .setExternalId(newRecordExternalId)
            .setMarketId(marketID)
            .setTargetId(azureTargetId)
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();
        final List<String> diags = Arrays.asList(
            GSON.toJson(planDestination)
        );
        try {
            planDestinationDaoImpl.restoreDiags(diags, null);
        } catch (DiagnosticsException e) {
            fail();
        }

        final List<PlanExportDTO.PlanDestination> result = planDestinationDaoImpl.getAllPlanDestinations();

        assertEquals(1, result.size());
        assertTrue(result.stream()
            .noneMatch(found -> found.equals(preexistingNone)));
        assertTrue(result.stream().noneMatch(found -> existingInProgressExternalId.equals(found.getExternalId())));
        assertTrue(result.stream().anyMatch(found -> newRecordExternalId.equals(found.getExternalId())));
    }

    /**
     * Test updatePlanDestination(oid, planDestination) Method.
     *
     * @throws IntegrityException db error.
     * @throws NoSuchObjectException can't find record in db.
     */
    @Test
    public void testUpdatePlanDestinationInDiscovery() throws IntegrityException, NoSuchObjectException {
        final long azureTargetId = 1233245L;
        final long marketID = 77777L;

        final PlanExportStatus.Builder planExportStatusInProgress = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setProgress(30);
        final String existingInProgressExternalId = "in progress external id";

        final PlanExportDTO.PlanDestination preexistingInProgress = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("preexisting in-process")
            .setStatus(planExportStatusInProgress)
            .setExternalId(existingInProgressExternalId)
            .setTargetId(azureTargetId)
            .setMarketId(marketID)
            .build();
        PlanExportDTO.PlanDestination inDbRecord = planDestinationDaoImpl.createPlanDestination(preexistingInProgress);

        PlanExportDTO.PlanDestination newDiscoveryRecord = PlanExportDTO.PlanDestination.newBuilder(inDbRecord)
            .setDisplayName("New DisplayName")
            .setMarketId(marketID + 1)
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();

        PlanExportDTO.PlanDestination result = planDestinationDaoImpl.updatePlanDestination(inDbRecord.getOid(), newDiscoveryRecord);

        assertNotNull(result);
        assertEquals(inDbRecord.getOid(), result.getOid());
        assertEquals(inDbRecord.getStatus(), result.getStatus());
        assertTrue(inDbRecord.hasMarketId());
        assertEquals(inDbRecord.getMarketId(), result.getMarketId());
        assertEquals(newDiscoveryRecord.getDisplayName(), result.getDisplayName());
    }

    /**
     * Test updatePlanDestination(oid, planDestination) Method. No MarketId for the record IN DB
     * to ensure the updated record preserve it.
     *
     * @throws IntegrityException db error.
     * @throws NoSuchObjectException can't find record in db.
     */
    @Test
    public void testUpdatePlanDestinationInDiscoveryNoMarketValueInDB() throws IntegrityException, NoSuchObjectException {
        final long azureTargetId = 1233245L;
        final long marketID = 77777L;

        final PlanExportStatus.Builder planExportStatusInProgress = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setProgress(30);
        final String existingInProgressExternalId = "in progress external id";

        final PlanExportDTO.PlanDestination preexistingInProgress = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("preexisting in-process")
            .setStatus(planExportStatusInProgress)
            .setExternalId(existingInProgressExternalId)
            .setTargetId(azureTargetId)
            .build();
        PlanExportDTO.PlanDestination inDbRecord = planDestinationDaoImpl.createPlanDestination(preexistingInProgress);

        PlanExportDTO.PlanDestination newDiscoveryRecord = PlanExportDTO.PlanDestination.newBuilder(inDbRecord)
            .setDisplayName("New DisplayName")
            .setMarketId(marketID + 1)
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();

        PlanExportDTO.PlanDestination result = planDestinationDaoImpl.updatePlanDestination(inDbRecord.getOid(), newDiscoveryRecord);

        assertNotNull(result);
        assertEquals(inDbRecord.getOid(), result.getOid());
        assertEquals(inDbRecord.getStatus(), result.getStatus());
        assertFalse(result.hasMarketId());
        assertEquals(newDiscoveryRecord.getDisplayName(), result.getDisplayName());
    }

    /**
     * Test updatePlanDestination(oid, planDestination) Method. MarketId for the record IN DB
     * but incoming one doesn't to ensure the updated record preserve it.
     *
     * @throws IntegrityException db error.
     * @throws NoSuchObjectException can't find record in db.
     */
    @Test
    public void testUpdatePlanDestinationInDiscoveryMarketValueInDB() throws IntegrityException, NoSuchObjectException {
        final long azureTargetId = 1233245L;
        final long marketID = 77777L;

        final PlanExportStatus.Builder planExportStatusInProgress = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setProgress(30);
        final String existingInProgressExternalId = "in progress external id";

        final PlanExportDTO.PlanDestination preexistingInProgress = PlanExportDTO.PlanDestination.newBuilder()
            .setDisplayName("preexisting in-process")
            .setStatus(planExportStatusInProgress)
            .setExternalId(existingInProgressExternalId)
            .setMarketId(marketID)
            .setTargetId(azureTargetId)
            .build();
        PlanExportDTO.PlanDestination inDbRecord = planDestinationDaoImpl.createPlanDestination(preexistingInProgress);

        PlanExportDTO.PlanDestination newDiscoveryRecord = PlanExportDTO.PlanDestination.newBuilder(inDbRecord)
            .setDisplayName("New DisplayName")
            .clearMarketId()
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();

        PlanExportDTO.PlanDestination result = planDestinationDaoImpl.updatePlanDestination(inDbRecord.getOid(), newDiscoveryRecord);

        assertNotNull(result);
        assertEquals(inDbRecord.getOid(), result.getOid());
        assertEquals(inDbRecord.getStatus(), result.getStatus());
        assertTrue(result.hasMarketId());
        assertEquals(inDbRecord.getMarketId(), result.getMarketId());
        assertEquals(newDiscoveryRecord.getDisplayName(), result.getDisplayName());
    }
}
