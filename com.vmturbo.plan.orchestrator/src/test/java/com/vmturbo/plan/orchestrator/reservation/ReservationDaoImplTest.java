package com.vmturbo.plan.orchestrator.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.plan.orchestrator.templates.TemplatesDaoImpl;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class ReservationDaoImplTest {
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

    private ReservationDaoImpl reservationDao;

    private TemplatesDao templatesDao;

    private Date firstStartDateTime;

    private Date firstEndDateTime;

    private Date secondStartDateTime;

    private Date secondEndDateTime;

    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(0);

        cal.set(2018, 12, 12, 0, 0);
        firstStartDateTime = cal.getTime();

        cal.set(2018, 12, 15, 0, 0);
        firstEndDateTime = cal.getTime();

        cal.set(2019, 11, 22, 0, 0);
        secondStartDateTime = cal.getTime();

        cal.set(2019, 12, 29, 0, 0);
        secondEndDateTime = cal.getTime();
    }


    private Reservation testFirstReservation = Reservation.newBuilder()
            .setName("Test-first-reservation")
                .setStartDate(firstStartDateTime.getTime())
            .setExpirationDate(firstEndDateTime.getTime())
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
            .setStartDate(secondStartDateTime.getTime())
            .setExpirationDate(secondEndDateTime.getTime())
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
        reservationDao = new ReservationDaoImpl(dbConfig.getDslContext());
        templatesDao = new TemplatesDaoImpl(dbConfig.getDslContext(), "emptyDefaultTemplates.json",
                new IdentityInitializer(0));
    }

    @Test
    public void testCreateReservation() throws DuplicateTemplateException {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        Optional<Reservation> retrievedReservation =
                reservationDao.getReservationById(createdReservation.getId());
        assertThat(retrievedReservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testCreateReservationWithoutConstraint() throws DuplicateTemplateException {
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
    public void testGetReservationById() throws DuplicateTemplateException {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        Optional<Reservation> reservation = reservationDao.getReservationById(createdReservation.getId());
        assertTrue(reservation.isPresent());
        assertThat(reservation.get(), Matchers.is(createdReservation));
    }

    /**
     * Test the blocking call of reservation.
     * @throws DuplicateTemplateException if template is duplicate.
     */
    @Test
    public void testGetReservationByIdBlocking() throws DuplicateTemplateException {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation createdReservation = reservationDao.createReservation(reservationWithTemplate);
        createdReservation = createdReservation.toBuilder().setStatus(ReservationStatus.RESERVED).build();
        try {
            reservationDao.updateReservation(createdReservation.getId(), createdReservation);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
        Optional<Reservation> reservation = reservationDao.getReservationById(createdReservation.getId(), true);
        assertTrue(reservation.isPresent());
        assertThat(reservation.get(), Matchers.is(createdReservation));
    }

    @Test
    public void testGetAllReservation() throws DuplicateTemplateException {
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
    public void testUpdateReservation() throws NoSuchObjectException, DuplicateTemplateException {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        Reservation reservationWithTemplateSecond = createReservationWithTemplate(testSecondReservation);
        Reservation reservation = reservationDao.createReservation(reservationWithTemplateFirst);
        Reservation newReservation = reservationDao.updateReservation(reservation.getId(),
                reservationWithTemplateSecond);
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertThat(retrievedReservation.get(), is(newReservation));
    }

    @Test
    public void testDeleteReservation() throws NoSuchObjectException, DuplicateTemplateException {
        Reservation reservationWithTemplate = createReservationWithTemplate(testFirstReservation);
        Reservation reservation = reservationDao.createReservation(reservationWithTemplate);
        Reservation deletedReservation = reservationDao.deleteReservationById(reservation.getId());
        assertThat(deletedReservation, is(reservation));
        Optional<Reservation> retrievedReservation = reservationDao.getReservationById(reservation.getId());
        assertFalse(retrievedReservation.isPresent());
    }

    @Test
    public void testGetReservationByStatus() throws DuplicateTemplateException {
        Reservation reservationWithTemplateFirst = createReservationWithTemplate(testFirstReservation);
        reservationDao.createReservation(reservationWithTemplateFirst);
        Set<Reservation> retrievedReservation = reservationDao.getReservationsByStatus(ReservationStatus.INITIAL);
        assertTrue(retrievedReservation.size() == 1);
        assertEquals("Test-first-reservation", retrievedReservation.iterator().next().getName());
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
    public void testGetReservationsByTemplates() throws DuplicateTemplateException {
        Template template = templatesDao.createTemplate(TemplateInfo.newBuilder()
                .setName("test-template")
                .build());
        Reservation reservation = updateReservationTemplate(testFirstReservation, template);
        reservationDao.createReservation(reservation);
        Set<Reservation> reservationSet =
                reservationDao.getReservationsByTemplates(Sets.newHashSet(template.getId()));
        assertEquals(1L, reservationSet.size());
        assertEquals("Test-first-reservation", reservationSet.iterator().next().getName());
    }

    private Reservation createReservationWithTemplate(@Nonnull final Reservation reservation) throws DuplicateTemplateException {
        Template template = templatesDao.createTemplate(TemplateInfo.newBuilder()
                .setName(reservation.getName())
                .build());
        return updateReservationTemplate(reservation, template);

    }

    private Reservation updateReservationTemplate(@Nonnull final Reservation reservation,
                                                  final Template template) {
        // make sure reservation only have 1 template.
        assertEquals(1,
                reservation.getReservationTemplateCollection().getReservationTemplateCount());
        Reservation.Builder builder = reservation.toBuilder();
        builder.getReservationTemplateCollectionBuilder()
                .getReservationTemplateBuilderList()
        .forEach(reservationTemplate -> reservationTemplate.setTemplateId(template.getId())
        .setTemplate(template));
        return builder.build();
    }

    @Test
    public void testCollectDiags() throws Exception {

        Reservation createdFirstReservation =
            reservationDao.createReservation(createReservationWithTemplate(testFirstReservation));
        Reservation createdSecondReservation =
            reservationDao.createReservation(createReservationWithTemplate(testSecondReservation));
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        reservationDao.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        final List<String> expected = Stream.of(createdFirstReservation, createdSecondReservation)
            .map(profile -> ReservationDaoImpl.GSON.toJson(profile, Reservation.class))
            .collect(Collectors.toList());

        assertTrue(diags.getAllValues().containsAll(expected));
        assertTrue(expected.containsAll(diags.getAllValues()));
    }

    @Test
    public void testRestoreFromDiags() throws Exception {

        final Reservation preexisting =
            reservationDao.createReservation(createReservationWithTemplate(testFirstReservation));

        final List<String> diags = Arrays.asList(
            "{\"id\":\"3143031140272\",\"name\":\"Test-first-reservation\",\"startDate\":"
                    + "\"1547251200000\",\"expirationDate\":\"1547510400000\",\"status\":\"INITIAL\""
                    + ",\"reservationTemplateCollection\":{\"reservationTemplate\":[{\"count\":\"1\","
                    + "\"templateId\":\"3143031138320\",\"reservationInstance\":[{\"entityId\":\"456\","
                    + "\"placementInfo\":[{\"providerId\":\"14\"}]}],"
                    + "\"template\":{\"id\":\"3143031138320\",\"templateInfo\":"
                    + "{\"name\":\"Test-first-reservation\"},\"type\":\"USER\"}}]}"
                    + ",\"constraintInfoCollection\":"
                    + "{\"reservationConstraintInfo\":[{\"constraintId\":\"100\",\"type\":"
                    + "\"DATA_CENTER\"}]}}",
            "{\"id\":\"1997938756368\",\"name\":\"Test-second-reservation\",\"startDate\":" +
                "\"1574398800000\",\"expirationDate\":\"1577595600000\",\"status\":\"RESERVED\"," +
                "\"reservationTemplateCollection\":{\"reservationTemplate\":[{\"count\":\"1\"," +
                "\"templateId\":\"1997938755584\",\"reservationInstance\":[{\"entityId\":\"456\"," +
                "\"placementInfo\":[{\"providerId\":\"14\"}]}]}]},\"constraintInfoCollection\":" +
                "{\"reservationConstraintInfo\":[{\"constraintId\":\"100\",\"type\":" +
                "\"DATA_CENTER\"}]}}"
        );
        try {
            reservationDao.restoreDiags(diags, null);
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
