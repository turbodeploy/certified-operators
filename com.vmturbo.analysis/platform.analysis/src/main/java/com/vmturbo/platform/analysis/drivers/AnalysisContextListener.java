package com.vmturbo.platform.analysis.drivers;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class represents servlet context listener, which, if specified in web.xml, will receive
 * {@link ServletContext} object when web application is started up.
 *
 * @author weiduan
 *
 */
public class AnalysisContextListener implements ServletContextListener {

    private static final Logger logger = LogManager.getLogger(AnalysisContextListener.class);

    // the configuration used for creating analysis server endpoint
    private AnalysisServerConfig analysisServerConfig;


    @Override
    public synchronized void contextInitialized(ServletContextEvent sce) {
        if (analysisServerConfig == null) {
            analysisServerConfig = new AnalysisServerConfig();
        } else {
            throw new IllegalArgumentException(
                            "Analysis server configuration has already been created");
        }
        try {
            analysisServerConfig.init((ServerContainer)sce.getServletContext()
                            .getAttribute(ServerContainer.class.getName()));
        } catch (DeploymentException e) {
            logger.error("Error opening analysis server websocket endpoint ", e);
        }
        logger.info("Analysis server websocket endpoint opened");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (analysisServerConfig != null) {
            analysisServerConfig.close();
        }
    }

}
