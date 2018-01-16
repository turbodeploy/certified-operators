package com.vmturbo.history.db;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;

/**
 * Spring Configuration for the HistorydbIO class.
 **/
@Configuration
public class HistoryDbConfig {


    @Value("${authHost:auth}")
    public String authHost;

    @Value("${authPort:8080}")
    public int authPort;

    @Bean
    public HistorydbIO historyDbIO() {
        final HistorydbIO dbIO = new HistorydbIO(dbPasswordUtil());
        HistorydbIO.setSharedInstance(dbIO);
        return dbIO;
    }

    @Bean
    public DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort);
    }


}
