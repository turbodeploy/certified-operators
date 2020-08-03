package com.vmturbo.plan.orchestrator;

import java.util.Optional;

import javax.sql.DataSource;

import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for plan-orchestrator component interaction with a schema.
 */
@Configuration
public class PlanOrchestratorDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${planDbUsername:plan}")
    private String planDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${planDbPassword:}")
    private String planDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:plan}")
    private String dbSchemaName;

    /**
     * Initialize plan-orchestrator DB config by running flyway migration and creating a user.
     *
     * @return DataSource of plan-orchestrator DB.
     */
    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, planDbUsername, Optional.ofNullable(
                !Strings.isEmpty(planDbPassword) ? planDbPassword : null));
    }

    /**
     * To avoid circular dependency between {@link com.vmturbo.plan.orchestrator.templates.TemplatesConfig} and
     * {@link com.vmturbo.plan.orchestrator.reservation.ReservationConfig}, initialize the reservation DAO here.
     *
     * @return reservation DAO.
     */
    @Bean
    public ReservationDao reservationDao() {
        return new ReservationDaoImpl(super.dsl());
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
