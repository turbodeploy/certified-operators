package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.time.Duration;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.SAML.SAMLCondition;
import com.vmturbo.api.component.external.api.SAML.SAMLUserDetailsServiceImpl;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.widgets.AuthClientConfig;
import com.vmturbo.reporting.api.ReportingClientConfig;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;


/**
 * Spring Configuration that initializes all the services.
 * <p>
 * For readability purposes, the services should appear in alphabetical order.
 */
@Configuration
@Import({SpringSecurityConfig.class, MapperConfig.class, CommunicationConfig.class,
        RepositoryClientConfig.class, ReportingClientConfig.class})
@PropertySource("classpath:api-component.properties")
public class ServiceConfig {

    @Autowired
    private AuthClientConfig authConfig;
    /**
     * A path prefix to access reports data (implemented in Legacy as CGI-script) using
     * {@link ReportCgiServlet}.
     */
    public static final String REPORT_CGI_PATH = "/cgi-bin/vmtreport.cgi";

    @Value("${targetValidationTimeoutSeconds}")
    private Long targetValidationTimeoutSeconds;

    @Value("${targetValidationPollIntervalSeconds}")
    private Long targetValidationPollIntervalSeconds;
    @Value("${initialPlacementTimeoutSeconds}")
    private  Long initialPlacementTimeoutSeconds;

    @Value("${supplyChainFetcherTimeoutSeconds}")
    private Long supplyChainFetcherTimeoutSeconds;

    /**
     * We allow autowiring between different configuration objects, but not for a bean.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private CommunicationConfig communicationConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private MapperConfig mapperConfig;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

    @Autowired
    private ReportingClientConfig reportingClientConfig;

    @Autowired
    private ServletContext servletContext;

    @Bean
    public ActionsService actionsService() {
        return new ActionsService(communicationConfig.actionsRpcService(),
                                  mapperConfig.actionSpecMapper(),
                                  communicationConfig.repositoryApi(),
                                  communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public AdminService adminService() {
        return new AdminService(clusterService());
    }

    @Bean
    public AuthenticationService authenticationService() {
        return new AuthenticationService(authConfig.getAuthHost(), authConfig.getAuthPort(),
                securityConfig.verifier(), communicationConfig.serviceRestTemplate());
    }

    //TODO, conditional load it, but need to enable conditional Autowire in {@link ApiSecurityConfig}.
    @Bean
    public SAMLUserDetailsService samlUserDetailsService() {
        return new SAMLUserDetailsServiceImpl();
    }

    @Bean
    public BusinessUnitsService businessUnitsService() {
        return new BusinessUnitsService();
    }

    @Bean
    public ClusterService clusterService() {
        return new ClusterService(communicationConfig.clusterMgr());
    }

    @Bean
    public DeploymentProfilesService deploymentProfilesService() {
        return new DeploymentProfilesService();
    }

    @Bean
    public EntitiesService entitiesService() {
        return new EntitiesService(
                communicationConfig.actionsRpcService(),
                mapperConfig.actionSpecMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.supplyChainFetcher()
        );
    }

    @Bean
    public GroupsService groupsService() {
        return new GroupsService(
                communicationConfig.actionsRpcService(),
                communicationConfig.groupRpcService(),
                mapperConfig.actionSpecMapper(),
                mapperConfig.groupMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                communicationConfig.templateServiceBlockingStub());
    }

    @Bean
    public LicenseService licenseService() {
        return new LicenseService(authConfig.getAuthHost(),
                authConfig.getAuthPort(),
                communicationConfig.serviceRestTemplate(),
                communicationConfig.licenseManagerStub(),
                communicationConfig.licenseCheckServiceStub());
    }

    @Bean
    public LogsService logsService() {
        return new LogsService();
    }

    @Bean
    public MarketsService marketsService() {
        return new MarketsService(mapperConfig.actionSpecMapper(),
                mapperConfig.uuidMapper(),
                communicationConfig.actionsRpcService(),
                communicationConfig.policyRpcService(),
                communicationConfig.planRpcService(),
                mapperConfig.policyMapper(),
                mapperConfig.marketMapper(),
                communicationConfig.groupRpcService(),
                communicationConfig.repositoryRpcService(),
                websocketConfig.websocketHandler());
    }

    @Bean
    public NotificationService notificationService() {
        return new NotificationService();
    }

    @Bean
    public PoliciesService policiesService() {
        return new PoliciesService(
                communicationConfig.policyRpcService(),
                communicationConfig.groupRpcService(), mapperConfig.policyMapper());
    }

    @Bean
    public ReservedInstancesService reservedInstancesService() {
        return new ReservedInstancesService();
    }

    @Bean
    public ReportsService reportsService() {
        return new ReportsService(reportingRpcService(), groupsService());
    }

    @Bean
    public ReportingServiceBlockingStub reportingRpcService() {
        return ReportingServiceGrpc.newBlockingStub(reportingClientConfig.reportingChannel());
    }

    @Bean
    public Servlet reportServlet() {
        final Servlet servlet = new ReportCgiServlet(reportingRpcService());
        final ServletRegistration.Dynamic registration =
                servletContext.addServlet("reports-cgi-servlet", servlet);
        registration.setLoadOnStartup(1);
        registration.addMapping(REPORT_CGI_PATH);
        return servlet;
    }

    @Bean
    public ReservationsService reservationsService() {
        return new ReservationsService(
                communicationConfig.reservationServiceBlockingStub(),
                mapperConfig.reservationMapper(),
                initialPlacementTimeoutSeconds,
                communicationConfig.planRpcService(),
                communicationConfig.planRpcServiceFuture(),
                communicationConfig.actionsRpcService(),
                communicationConfig.templateServiceBlockingStub());
    }

    @Bean
    public RolesService rolesService() {
        return new RolesService();
    }

    @Bean
    public NotificationSettingsService notificationSettingsService() {
        return new NotificationSettingsService();
    }

    @Bean
    public ScenariosService scenariosService() {
        return new ScenariosService(communicationConfig.planOrchestratorChannel(),
                mapperConfig.scenarioMapper());
    }

    @Bean
    public SearchService searchService() {
        return new SearchService(
                communicationConfig.repositoryApi(),
                marketsService(),
                groupsService(),
                targetService(),
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.entitySeverityService(),
                communicationConfig.groupExpander(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.groupMapper(),
                mapperConfig.groupUseCaseParser(),
                mapperConfig.uuidMapper(),
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public SettingsService settingsService() {
        return new SettingsService(communicationConfig.settingRpcService(),
                communicationConfig.historyRpcService(),
                mapperConfig.settingsMapper(),
                mapperConfig.settingManagerMappingLoader().getMapping());
    }

    @Bean
    public SettingsPoliciesService settingsPoliciesService() {
        return new SettingsPoliciesService(mapperConfig.settingsMapper(),
                communicationConfig.groupChannel());
    }

    @Bean
    public StatsService statsService() {
        return new StatsService(communicationConfig.historyRpcService(),
                communicationConfig.planRpcService(),
                communicationConfig.repositoryApi(),
                repositoryClientConfig.repositoryClient(),
                communicationConfig.supplyChainFetcher(),
                communicationConfig.groupExpander(),
                Clock.systemUTC(),
                targetService(),
                communicationConfig.groupRpcService());
    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(communicationConfig.supplyChainFetcher(),
                communicationConfig.planRpcService(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.groupExpander());
    }

    @Bean
    public TargetsService targetService() {
        return new TargetsService(communicationConfig.topologyProcessor(),
                Duration.ofSeconds(targetValidationTimeoutSeconds),
                Duration.ofSeconds(targetValidationPollIntervalSeconds));
    }

    @Bean
    public TemplatesService templatesService() {
        return new TemplatesService(communicationConfig.templateServiceBlockingStub(),
                                    mapperConfig.templateMapper(),
                                    communicationConfig.templateSpecServiceBlockingStub());
    }

    @Bean
    public UsersService usersService() {
        return new UsersService(authConfig.getAuthHost(),
                                authConfig.getAuthPort(),
                                communicationConfig.serviceRestTemplate());
    }

    @Bean
    public WidgetSetsService widgetSetsService() {
        return new WidgetSetsService(communicationConfig.widgetsetsServiceBlockingStub());
    }
}
