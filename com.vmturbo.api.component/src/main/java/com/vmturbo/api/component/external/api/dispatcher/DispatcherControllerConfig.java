package com.vmturbo.api.component.external.api.dispatcher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.component.external.api.ApiSecurityConfig;
import com.vmturbo.api.component.external.api.service.LicenseService;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.controller.ActionsController;
import com.vmturbo.api.controller.AdminController;
import com.vmturbo.api.controller.AuthenticationController;
import com.vmturbo.api.controller.BusinessUnitsController;
import com.vmturbo.api.controller.DeploymentProfilesController;
import com.vmturbo.api.controller.EntitiesController;
import com.vmturbo.api.controller.GeneralController;
import com.vmturbo.api.controller.GroupsController;
import com.vmturbo.api.controller.LicensesController;
import com.vmturbo.api.controller.LogsController;
import com.vmturbo.api.controller.MarketsController;
import com.vmturbo.api.controller.NotificationsController;
import com.vmturbo.api.controller.PoliciesController;
import com.vmturbo.api.controller.ReportsController;
import com.vmturbo.api.controller.ReservationsController;
import com.vmturbo.api.controller.ReservedInstancesController;
import com.vmturbo.api.controller.RolesController;
import com.vmturbo.api.controller.ScenariosController;
import com.vmturbo.api.controller.SearchController;
import com.vmturbo.api.controller.SettingsController;
import com.vmturbo.api.controller.SettingsPoliciesController;
import com.vmturbo.api.controller.StatsController;
import com.vmturbo.api.controller.SupplyChainsController;
import com.vmturbo.api.controller.TargetsController;
import com.vmturbo.api.controller.TemplatesController;
import com.vmturbo.api.controller.UsersController;
import com.vmturbo.api.controller.WidgetSetsController;
import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.validators.TemplatesValidator;
import com.vmturbo.api.xlcontroller.ClusterController;
import com.vmturbo.api.xlcontroller.SAMLController;

/**
 * Configuration for the dispatcher servlet responsible for
 * handling calls to the external REST API. And also create
 * Swagger Docket for runtime swagger API documentation which
 * controllers are instantiated in child Spring context.
 *
 * The controllers here should match those in
 * com.vmturbo.api.controller and com.vmturbo.api.controller.
 *
 * Import Spring security configuration from ApiSecurityConfig to enable method access-control for
 * controllers.
 * E.g.  @PreAuthorize("hasRole('ADMINISTRATOR')").
 *
 */
@Configuration
@EnableWebMvc
@EnableWebSecurity
@Import({ApiSecurityConfig.class, SecurityChainProxyInvoker.class})
// DO NOT import configurations outside the external.api.dispatcher package here, because
// that will re-create the configuration's beans in the child context for the dispatcher servlet.
// You will end up with multiple instances of the same beans, which could lead to tricky bugs.
// Note: ApiSecurityConfig doesn't have beans.
public class DispatcherControllerConfig extends WebMvcConfigurerAdapter {

    /**
     * This should get wired in from the root context.
     */
    @Autowired
    public ServiceConfig serviceConfig;

    @Bean
    public ActionsController actionsController() {
        return new ActionsController();
    }

    @Bean
    public AdminController adminController() {
        return new AdminController();
    }

    @Bean
    public AuthenticationController authenticationController() {
        return new AuthenticationController(serviceConfig.authenticationService());
    }

    @Bean
    public DeploymentProfilesController deploymentProfilesController() {
        return new DeploymentProfilesController();
    }

    @Bean
    public EntitiesController entitiesController() {
        return new EntitiesController();
    }

    @Bean
    public GeneralController generalController() {
        return new GeneralController();
    }

    @Bean
    public GroupsController groupsController() {
        return new GroupsController();
    }

    @Bean
    public LogsController logsController() {
        return new LogsController();
    }

    @Bean
    public MarketsController marketsController() {
        return new MarketsController();
    }

    @Bean
    public NotificationsController notificationsController() {
        return new NotificationsController();
    }

    @Bean
    public PoliciesController policiesController() {
        return new PoliciesController();
    }

    @Bean
    public ReportsController reportsController() {
        return new ReportsController();
    }

    @Bean
    public ReservedInstancesController reservedInstancesController() {
        return new ReservedInstancesController();
    }

    @Bean
    public ScenariosController scenariosController() {
        return new ScenariosController();
    }

    @Bean
    public SearchController searchController() {
        return new SearchController();
    }

    @Bean
    public SettingsController settingsController() {
        return new SettingsController();
    }

    @Bean
    public SettingsPoliciesController settingsPoliciesController() {
        return new SettingsPoliciesController();
    }

    @Bean
    public StatsController statsController() {
        return new StatsController();
    }

    @Bean
    public SupplyChainsController supplyChainsController() {
        return new SupplyChainsController();
    }

    @Bean
    public TargetsController targetsController() {
        return new TargetsController();
    }

    @Bean
    public TemplatesController templatesController() {
        return new TemplatesController(serviceConfig.templatesService());
    }

    @Bean
    public TemplatesValidator templatesValidator() {
        return new TemplatesValidator(serviceConfig.templatesService());
    }

    @InitBinder
    protected void initBinder(WebDataBinder binder) {
        binder.addValidators(templatesValidator());
    }

    @Bean
    public UsersController usersController() {
        return new UsersController();
    }

    @Bean
    public WidgetSetsController widgetSetsController() {
        return new WidgetSetsController();
    }

    @Bean
    public ClusterController clusterController() {
        return new ClusterController();
    }

    @Bean
    public LicensesController licenseController() {
        return new LicensesController(serviceConfig.licenseService());
    }

    @Bean
    public ReservationsController reservationsController() {
        return new ReservationsController();
    }

    @Bean
    public RolesController rolesController() {
        return new RolesController();
    }

    @Bean
    public BusinessUnitsController businessUnitsController() {
        return new BusinessUnitsController();
    }

    @Bean
    public GlobalExceptionHandler globalExceptionHandler() {
        return new GlobalExceptionHandler();
    }

    @Bean
    public SAMLController samlController() {
        return new SAMLController();
    }

}
