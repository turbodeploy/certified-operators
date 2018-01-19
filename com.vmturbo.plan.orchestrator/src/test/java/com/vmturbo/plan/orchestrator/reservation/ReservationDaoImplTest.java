package com.vmturbo.plan.orchestrator.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.Set;

import org.flywaydb.core.Flyway;
import org.hamcrest.Matchers;
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
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation.Date;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
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

    private Reservation testFirstReservation = Reservation.newBuilder()
            .setName("Test-first-reservation")
                .setStartDate(Date.newBuilder()
                        .setYear(2018)
                        .setMonth(12)
                        .setDay(12))
            .setExpirationDate(Date.newBuilder()
                        .setYear(2018)
                        .setMonth(12)
                        .setDay(25))
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
            .setStartDate(Date.newBuilder()
                    .setYear(2019)
                    .setMonth(11)
                    .setDay(22))
            .setExpirationDate(Date.newBuilder()
                    .setYear(2019)
                    .setMonth(12)
                    .setDay(29))
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
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreateReservation() {
        Reservation createdReservation = reservationDao.createReservation(testFirstReservation);
        Optional<Reservation> retrievedReservation =
                reservationDao.getReservationById(createdReservation.getId());
        assertThat(retrievedReservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testCreateReservationWithoutConstraint() {
        Reservation reservationWithoutConstraint = testFirstReservation.toBuilder()
                .clearConstraintInfoCollection()
                .build();
        Reservation createdReservation = reservationDao.createReservation(reservationWithoutConstraint);
        Optional<Reservation> retrievedReservation =
                reservationDao.getReservationById(createdReservation.getId());
        assertTrue(retrievedReservation.isPresent());
        assertTrue(retrievedReservation.get()
                .getConstraintInfoCollection()
                .getReservationConstraintInfoCount() == 0);
    }

    @Test
    public void testGetReservationById() {
        Reservation createdReservation = reservationDao.createReservation(testFirstReservation);
        Optional<Reservation> reservation = reservationDao.getReservationById(createdReservation.getId());
        assertTrue(reservation.isPresent());
        assertThat(reservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testGetAllReservation() {
        Reservation createdFirstReservation = reservationDao.createReservation(testFirstReservation);
        Reservation createdSecondReservation = reservationDao.createReservation(testSecondReservation);
        Set<Reservation> retrievedReservations = reservationDao.getAllReservations();
        assertTrue(retrievedReservations.size() == 2);
        assertTrue(retrievedReservations.stream()
                .anyMatch(template -> template.getId() == createdFirstReservation.getId()));
        assertTrue(retrievedReservations.stream()
                .anyMatch(template -> template.getId() == createdSecondReservation.getId()));
    }

    @Test
    public void testUpdateReservation() throws NoSuchObjectException {
        Reservation reservation = reservationDao.createReservation(testFirstReservation);
        Reservation newReservation = reservationDao.updateReservation(reservation.getId(),
                testSecondReservation);
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertThat(retrievedReservation.get(), is(newReservation));
    }

    @Test
    public void testDeleteReservation() throws NoSuchObjectException {
        Reservation reservation = reservationDao.createReservation(testFirstReservation);
        Reservation deletedReservation = reservationDao.deleteReservationById(reservation.getId());
        assertThat(deletedReservation, is(reservation));
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertFalse(retrievedReservation.isPresent());
    }

    @Test
    public void testGetReservationByStatus() {
        Reservation createdFirstReservation = reservationDao.createReservation(testFirstReservation);
        Reservation createdSecondReservation = reservationDao.createReservation(testSecondReservation);
        Set<Reservation> retrievedReservation = reservationDao.getReservationsByStatus(ReservationStatus.RESERVED);
        assertTrue(retrievedReservation.size() == 1);
        assertEquals("Test-second-reservation", retrievedReservation.iterator().next().getName());
    }

    @Test
    public void testUpdateReservationBatch() throws Exception {
        Reservation createdFirstReservation = reservationDao.createReservation(testFirstReservation);
        Reservation createdSecondReservation = reservationDao.createReservation(testSecondReservation);
        Reservation newFirstReservation = Reservation.newBuilder(testFirstReservation)
                .setId(createdFirstReservation.getId())
                .setName("New-first-reservation")
                .build();
        Reservation newSecondReservation = Reservation.newBuilder(testSecondReservation)
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
}
