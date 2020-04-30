package com.vmturbo.plan.orchestrator;

import javax.sql.DataSource;

import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
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
        // If no db password specified, use root password by default.
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);
        String dbPassword = !Strings.isEmpty(planDbPassword) ?
            planDbPassword : dbPasswordUtil.getSqlDbRootPassword();
        return dataSourceConfig(dbSchemaName, planDbUsername, dbPassword);
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
