package com.vmturbo.history.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Spring Configuration for the HistorydbIO class.
 **/
@Configuration
@Import({SQLDatabaseConfig.class})
public class HistoryDbConfig {


    @Value("${authHost}")
    public String authHost;

    @Value("${serverHttpPort}")
    public int authPort;

    @Value("${authRetryDelaySecs}")
    public int authRetryDelaySecs;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public HistorydbIO historyDbIO() {
        final HistorydbIO dbIO
            = new HistorydbIO(dbPasswordUtil(), databaseConfig.getSQLConfigObject());
        HistorydbIO.setSharedInstance(dbIO);
        return dbIO;
    }

    @Bean
    public DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRetryDelaySecs);
    }


}
