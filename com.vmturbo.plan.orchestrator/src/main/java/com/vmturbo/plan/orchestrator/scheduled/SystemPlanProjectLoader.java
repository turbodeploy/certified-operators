package com.vmturbo.plan.orchestrator.scheduled;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jooq.exception.DataAccessException;

import org.springframework.dao.DataAccessResourceFailureException;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectInfoNotFoundException;
import com.vmturbo.plan.orchestrator.project.PlanProjectNotFoundException;

/**
 * This class creates default system plan projects, which are specified in a JSON file.
 * All new plan projects created will also be scheduled.
 *
 * We assume there will only be 1 plan project of each type (e.g. HEADROOM).
 * If a project with the type of the system project already exists in the database,
 * the project will not be created and the data in the database will not be overwritten.
 */
public class SystemPlanProjectLoader {
    private final String defaultHeadroomPlanProjectJsonFile;
    private final Logger logger = LogManager.getLogger();
    private final PlanProjectDao planProjectDao;
    private final PlanProjectScheduler planProjectScheduler;

    final static long TIME_BETWEEN_DB_CONNECTION_TEST = 10000; // in milliseconds
    final static int MAX_NUM_OF_DB_CONNEDTION_TESTS = 180;

    /**
     * Contstructs a SystemPlanProjectLoader
     *
     * @param planProjectDao Data access object for plan_project table
     * @param planProjectScheduler plan project scheduler
     * @param defaultHeadroomPlanProjectJsonFile path to JSON configuration file in classpath
     */
    public SystemPlanProjectLoader(@Nonnull PlanProjectDao planProjectDao,
                                   @Nonnull PlanProjectScheduler planProjectScheduler,
                                   @Nonnull String defaultHeadroomPlanProjectJsonFile) throws InterruptedException {
        this.planProjectDao = planProjectDao;
        this.planProjectScheduler = planProjectScheduler;
        this.defaultHeadroomPlanProjectJsonFile = defaultHeadroomPlanProjectJsonFile;
        createAndScheduleDefaultSystemPlanProjects();
    }

    /**
     * Read the JSON config file and load data into a List of {@link PlanDTO.PlanProjectInfo} objects
     *
     * @return a list of PlanProjectInfo objects
     */
    private List<PlanDTO.PlanProjectInfo> getDefaultSystemPlanProjectInfo() {
        PlanDTO.PlanProjectInfoCollection.Builder collectionBuilder = PlanDTO.PlanProjectInfoCollection.newBuilder();

        // open the file and create a reader for it
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(defaultHeadroomPlanProjectJsonFile);
             InputStreamReader reader = new InputStreamReader(inputStream);
        ) {

            // parse the json file
            JsonFormat.parser().merge(reader, collectionBuilder);

        } catch (IOException e) {
            logger.error("Unable to load system plan projects from Json file: {}",
                    defaultHeadroomPlanProjectJsonFile, e);
        }

        final PlanDTO.PlanProjectInfoCollection planProjectInfoCollection = collectionBuilder.build();

        return planProjectInfoCollection.getPlanProjectInfoList();
    }

    /**
     * This method creates and schedule any system plan projects that don't exist in the database.
     */
    private void createAndScheduleDefaultSystemPlanProjects() throws InterruptedException {
        // wait for database to come up
        waitTillDatabaseComeUp();

        // Read the list of system plan projects from the JSON file.
        List<PlanDTO.PlanProjectInfo> planProjectInfos = getDefaultSystemPlanProjectInfo();

        for (PlanDTO.PlanProjectInfo planProjectInfo : planProjectInfos) {
            PlanDTO.PlanProjectInfo.PlanProjectType projectType = planProjectInfo.getType();
            List<PlanDTO.PlanProject> planProjects = planProjectDao.getPlanProjectsByType(projectType);
            // Only create the plan project if it does not already exist.
            if (planProjects.isEmpty()) {
                logger.info("Creating plan project {}.", planProjectInfo.getName());
                // create the project
                try {
                    PlanDTO.PlanProject planProject = planProjectDao.createPlanProject(planProjectInfo);
                    planProjectScheduler.setPlanProjectSchedule(planProject.getPlanProjectId());
                    logger.info("Plan scheduler successfully scheduled plan: {}",
                            planProjectInfo.getName());
                } catch (DataAccessException e) {
                    logger.error("Failed to create system plan project {}: {}",
                            planProjectInfo.getName(), e.getMessage());
                } catch (PlanProjectNotFoundException e) {
                    logger.error("Failed to schedule plan project {}: {}",
                            planProjectInfo.getName(),
                            e.getMessage());
                } catch (PlanProjectInfoNotFoundException e) {
                    logger.error("Failed to schedule plan project {}: {}",
                            planProjectInfo.getName(),
                            e.getMessage());
                }
            }
        }
    }

    /**
     * This method will block until database is available.
     * Database connection is tested by a simple select query on the plan_project table.
     * If select returns without exception, database is available.
     */
    private void waitTillDatabaseComeUp() throws InterruptedException {
        for (int i = 0; i < MAX_NUM_OF_DB_CONNEDTION_TESTS; i++) {
            try {
                planProjectDao.getPlanProjectsByType(PlanDTO.PlanProjectInfo.PlanProjectType.CLUSTER_HEADROOM);
                break;
            } catch (DataAccessResourceFailureException e) {
                // the select query failed. Database connection failed.
                // Wait for 10 seconds and try again.
                Thread.sleep(TIME_BETWEEN_DB_CONNECTION_TEST);
            }
        }
    }
}
