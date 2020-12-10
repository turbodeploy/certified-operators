package com.vmturbo.group.group.pagination;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class that holds pagination related values.
 */
@Configuration
public class GroupPaginationConfig {
    /**
     * The default limit for pagination for group component, to be applied to paginated requests
     * that do not specify a pagination limit.
     */
    @Value("${groupPaginationDefaultLimit:100}")
    private int groupPaginationDefaultLimit;

    /**
     * The maximum page limit for group component. If a paginated request has a limit that exceeds
     * this value, it will be discarded and this will be used instead.
     */
    @Value("${groupPaginationMaxLimit:500}")
    private int groupPaginationMaxLimit;

    /**
     * Creates a new {@link GroupPaginationParams} object.
     * @return the new object
     */
    @Bean
    public GroupPaginationParams groupPaginationParams() {
        return new GroupPaginationParams(groupPaginationDefaultLimit, groupPaginationMaxLimit);
    }
}
