package com.vmturbo.api.component.external.api.dispatcher;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import com.vmturbo.api.controller.ActionsController;
import com.vmturbo.api.controller.AdminController;
import com.vmturbo.api.controller.AuthenticationController;
import com.vmturbo.api.controller.BusinessUnitsController;
import com.vmturbo.api.controller.DeploymentProfilesController;
import com.vmturbo.api.controller.EntitiesController;
import com.vmturbo.api.controller.GeneralController;
import com.vmturbo.api.controller.GroupsController;
import com.vmturbo.api.controller.LicenseController;
import com.vmturbo.api.controller.LogsController;
import com.vmturbo.api.controller.MarketsController;
import com.vmturbo.api.controller.NotificationsController;
import com.vmturbo.api.controller.PoliciesController;
import com.vmturbo.api.controller.ReportsController;
import com.vmturbo.api.controller.ReservationsController;
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
import com.vmturbo.api.xlcontroller.ClusterController;

/**
 * Configuration for the dispatcher servlet responsible for
 * handling calls to the external REST API. And also create
 * Swagger Docket for runtime swagger API documentation which
 * controllers are instantiated in child Spring context.
 *
 * The controllers here should match those in
 * com.vmturbo.api.controller and com.vmturbo.api.controller.
 */
@Configuration
@EnableWebMvc
@EnableSwagger2
public class DispatcherControllerConfig extends WebMvcConfigurerAdapter {
    /**
     * Create the docket that defines runtime swagger API generation for
     * swagger-ui. The generated API documentations will contains controllers
     * instantiated in this child Spring context.
     * @return The {@link Docket} to use for swagger API generation.
     */
    @Bean
    public Docket createSwaggerDocket() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.vmturbo"))
                .paths(PathSelectors.any())
                .build()
                .apiInfo(apiInfo())
                .pathMapping("/vmturbo/api/v2");
        // TODO: inject common security scheme here
    }

    /**
     * Define the user-facing information for the Swagger Package for the main REST API.
     *
     * @return the ApiInfo defining the user-facing fields for this Swagger package.
     */
    private ApiInfo apiInfo() {
        // TODO: fill out the legal stuff, e.g. the contact() information, etc.
        return new ApiInfoBuilder()
                .title("Turbonomic REST API")
                .description("This is the Turbonomic REST API")
                .version("2.0.0")
                .build();
    }

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
        return new AuthenticationController();
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
        return new TemplatesController();
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
    public LicenseController licenseController() {
        return new LicenseController();
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
}
