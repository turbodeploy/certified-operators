package com.vmturbo.plan.orchestrator.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.plan.orchestrator.templates.TemplatesDaoImpl;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class ReservationDaoImplTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private ReservationDaoImpl reservationDao;

    private TemplatesDao templatesDao;

    private DateTime firstStartDateTime =
            new DateTime(2018, 12, 12, 0, 0);

    private DateTime firstEndDateTime =
            new DateTime(2018, 12, 15, 0, 0);

    private DateTime secondStartDateTime =
            new DateTime(2019, 11, 22, 0, 0);

    private DateTime secondEndDateTime =
            new DateTime(2019, 12, 29, 0, 0);


    private Reservation testFirstReservation = Reservation.newBuilder()
            .setName("Test-first-reservation")
                .setStartDate(firstStartDateTime.getMillis())
            .setExpirationDate(firstEndDateTime.getMillis())
            .setStatus(ReservationStatus.FUTURE)
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationTemplate.newBuilder()
                                .setCount(1)
                                .setTemplateId(123)
                                .addReservationInstance(ReservationInstance.newBuilder()
                                        .setEntityId(456)
                                        .addPlacementInfo(PlacementInfo.newBuilder()
                                                .setProviderId(678)
                                                .setProviderId(14)))))
            .setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                        .addReservationConstraintInfo(ReservationConstraintInfo.newBuilder()
                                .setConstraintId(100)
                                .setType(Type.DATA_CENTER)))
            .build();

    private Reservation testSecondReservation = Reservation.newBuilder()
            .setName("Test-second-reservation")
            .setStartDate(secondStartDateTime.getMillis())
            .setExpirationDate(secondEndDateTime.getMillis())
            .setStatus(ReservationStatus.RESERVED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1)
                            .setTemplateId(123)
                            .addReservationInstance(ReservationInstance.newBuilder()
                                    .setEntityId(456)
                                    .addPlacementInfo(PlacementInfo.newBuilder()
                                            .setProviderId(678)
                                            .setProviderId(14)))))
            .setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                    .addReservationConstraintInfo(ReservationConstraintInfo.newBuilder()
                            .setConstraintId(100)
                            .setType(Type.DATA_CENTER)))
            .build();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservationDao = new ReservationDaoImpl(dsl);
        templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json",
                new IdentityInitializer(0));
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreateReservation() {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        Optional<Reservation> retrievedReservation =
                reservationDao.getReservationById(createdReservation.getId());
        assertThat(retrievedReservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testCreateReservationWithoutConstraint() {
        Reservation reservationWithoutConstraint = testFirstReservation.toBuilder()
                .clearConstraintInfoCollection()
                .build();
        Reservation reservationWithTemplate = createReservationWithTemplate(reservationWithoutConstraint);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        Optional<Reservation> retrievedReservation =
                reservationDao.getReservationById(createdReservation.getId());
        assertTrue(retrievedReservation.isPresent());
        assertTrue(retrievedReservation.get()
                .getConstraintInfoCollection()
                .getReservationConstraintInfoCount() == 0);
    }

    @Test
    public void testGetReservationById() {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        Optional<Reservation> reservation = reservationDao.getReservationById(createdReservation.getId());
        assertTrue(reservation.isPresent());
        assertThat(reservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testGetAllReservation() {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        Reservation reservationWithTemplateSecond = createReservationWithTemplate(testSecondReservation);
        Reservation createdFirstReservation = reservationDao.createReservation(reservationWithTemplateFirst);
        Reservation createdSecondReservation = reservationDao.createReservation(reservationWithTemplateSecond);
        Set<Reservation> retrievedReservations = reservationDao.getAllReservations();
        assertTrue(retrievedReservations.size() == 2);
        assertTrue(retrievedReservations.stream()
                .anyMatch(template -> template.getId() == createdFirstReservation.getId()));
        assertTrue(retrievedReservations.stream()
                .anyMatch(template -> template.getId() == createdSecondReservation.getId()));
    }

    @Test
    public void testUpdateReservation() throws NoSuchObjectException {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        Reservation reservationWithTemplateSecond = createReservationWithTemplate(testSecondReservation);
        Reservation reservation = reservationDao.createReservation(reservationWithTemplateFirst);
        Reservation newReservation = reservationDao.updateReservation(reservation.getId(),
                reservationWithTemplateSecond);
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertThat(retrievedReservation.get(), is(newReservation));
    }

    @Test
    public void testDeleteReservation() throws NoSuchObjectException {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation reservation = reservationDao.createReservation(reservationWithTemplate);
        Reservation deletedReservation = reservationDao.deleteReservationById(reservation.getId());
        assertThat(deletedReservation, is(reservation));
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertFalse(retrievedReservation.isPresent());
    }

    @Test
    public void testGetReservationByStatus() {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        Reservation reservationWithTemplateSecond = createReservationWithTemplate(testSecondReservation);
        reservationDao.createReservation(reservationWithTemplateFirst);
        reservationDao.createReservation(reservationWithTemplateSecond);
        Set<Reservation> retrievedReservation = reservationDao.getReservationsByStatus(ReservationStatus.RESERVED);
        assertTrue(retrievedReservation.size() == 1);
        assertEquals("Test-second-reservation", retrievedReservation.iterator().next().getName());
    }

    @Test
    public void testUpdateReservationBatch() throws Exception {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        Reservation reservationWithTemplateSecond = createReservationWithTemplate(testSecondReservation);
        Reservation createdFirstReservation = reservationDao.createReservation(reservationWithTemplateFirst);
        Reservation createdSecondReservation = reservationDao.createReservation(reservationWithTemplateSecond);
        Reservation newFirstReservation = Reservation.newBuilder(createdFirstReservation)
                .setId(createdFirstReservation.getId())
                .setName("New-first-reservation")
                .build();
        Reservation newSecondReservation = Reservation.newBuilder(createdSecondReservation)
                .setId(createdSecondReservation.getId())
                .setName("New-second-reservation")
                .build();
        Set<Reservation> retrievedReservation = reservationDao.updateReservationBatch(
                Sets.newHashSet(newFirstReservation, newSecondReservation));
        assertEquals(2, retrievedReservation.size());
        assertTrue(retrievedReservation.stream()
                .anyMatch(reservation ->  reservation.getName().contains("New-first-reservation")));
        assertTrue(retrievedReservation.stream()
                .anyMatch(reservation ->  reservation.getName().contains("New-second-reservation")));
    }

    @Test
    public void testGetReservationsByTemplates() {
        Template template = templatesDao.createTemplate(TemplateInfo.newBuilder()
                .setName("test-template")
                .build());
        Reservation reservation = updateReservationTemplate(testFirstReservation, template.getId());
        reservationDao.createReservation(reservation);
        Set<Reservation> reservationSet =
                reservationDao.getReservationsByTemplates(Sets.newHashSet(template.getId()));
        assertEquals(1L, reservationSet.size());
        assertEquals("Test-first-reservation", reservationSet.iterator().next().getName());
    }

    private Reservation createReservationWithTemplate(@Nonnull final Reservation reservation) {
        Template template = templatesDao.createTemplate(TemplateInfo.newBuilder()
                .setName("test-template")
                .build());
        return updateReservationTemplate(reservation, template.getId());

    }

    private Reservation updateReservationTemplate(@Nonnull final Reservation reservation,
                                                  final long templateId) {
        // make sure reservation only have 1 template.
        assertEquals(1,
                reservation.getReservationTemplateCollection().getReservationTemplateCount());
        Reservation.Builder builder = reservation.toBuilder();
        builder.getReservationTemplateCollectionBuilder()
                .getReservationTemplateBuilderList()
                .forEach(reservationTemplate -> reservationTemplate.setTemplateId(templateId));
        return builder.build();
    }

    @Test
    public void testCollectDiags() throws Exception {

        Reservation createdFirstReservation =
            reservationDao.createReservation(createReservationWithTemplate(testFirstReservation));
        Reservation createdSecondReservation =
            reservationDao.createReservation(createReservationWithTemplate(testSecondReservation));
        final List<String> result = reservationDao.collectDiags();

        final List<String> expected = Stream.of(createdFirstReservation, createdSecondReservation)
            .map(profile -> ReservationDaoImpl.GSON.toJson(profile, Reservation.class))
            .collect(Collectors.toList());

        assertTrue(result.containsAll(expected));
        assertTrue(expected.containsAll(result));
    }

    @Test
    public void testRestoreFromDiags() throws Exception {

        final Reservation preexisting =
            reservationDao.createReservation(createReservationWithTemplate(testFirstReservation));

        final List<String> diags = Arrays.asList(
            "{\"id\":\"1997938755808\",\"name\":\"Test-first-reservation\",\"startDate\":" +
                "\"1544590800000\",\"expirationDate\":\"1544850000000\",\"status\":\"FUTURE\"," +
                "\"reservationTemplateCollection\":{\"reservationTemplate\":[{\"count\":\"1\"," +
                "\"templateId\":\"1997938753776\",\"reservationInstance\":[{\"entityId\":\"456\"," +
                "\"placementInfo\":[{\"providerId\":\"14\"}]}]}]},\"constraintInfoCollection\":" +
                "{\"reservationConstraintInfo\":[{\"constraintId\":\"100\",\"type\":" +
                "\"DATA_CENTER\"}]}}",
            "{\"id\":\"1997938756368\",\"name\":\"Test-second-reservation\",\"startDate\":" +
                "\"1574398800000\",\"expirationDate\":\"1577595600000\",\"status\":\"RESERVED\"," +
                "\"reservationTemplateCollection\":{\"reservationTemplate\":[{\"count\":\"1\"," +
                "\"templateId\":\"1997938755584\",\"reservationInstance\":[{\"entityId\":\"456\"," +
                "\"placementInfo\":[{\"providerId\":\"14\"}]}]}]},\"constraintInfoCollection\":" +
                "{\"reservationConstraintInfo\":[{\"constraintId\":\"100\",\"type\":" +
                "\"DATA_CENTER\"}]}}"
        );
        try {
            reservationDao.restoreDiags(diags);
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting reservations"));
        }

        final Set<Reservation> result = reservationDao.getAllReservations();
        assertEquals(2, result.size());
        assertFalse(result.contains(preexisting));
        final List<Reservation> expected = diags.stream()
            .map(serial -> ReservationDaoImpl.GSON.fromJson(serial, Reservation.class))
            .collect(Collectors.toList());
        assertTrue(expected.containsAll(result));
        assertTrue(result.containsAll(expected));

    }
}
