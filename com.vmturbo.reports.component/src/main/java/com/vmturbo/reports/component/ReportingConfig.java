package com.vmturbo.reports.component;

import org.eclipse.birt.core.exception.BirtException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.component.templates.TemplatesDaoImpl;

/**
 * Spring beans configuration for running reporting.
 */
@Configuration
@Import({ReportingDbConfig.class})
public class ReportingConfig {

    @Autowired
    private ReportingDbConfig dbConfig;

    @Bean
    public ComponentReportRunner componentReportRunner() {
        try {
            return new ComponentReportRunner();
        } catch (BirtException e) {
            throw new BeanCreationException("Could not create component report runner", e);
        }
    }

    @Bean
    public ReportingServiceRpc reportingService() {
        return new ReportingServiceRpc(componentReportRunner(), templatesDao());
    }

    @Bean
    public TemplatesDao templatesDao() {
        return new TemplatesDaoImpl(dbConfig.dsl());
    }
}
