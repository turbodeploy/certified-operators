package com.vmturbo.cost.component;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.rpc.RIAndExpenseUploadRpcService;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({CostConfig.class,
    ReservedInstanceConfig.class,
    DbAccessConfig.class,
    ReservedInstanceSpecConfig.class
})
public class CostServiceConfig {
    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    @Value("${riSupportInPartialCloudEnvironment:true}")
    private boolean riSupportInPartialCloudEnvironment;

    @Bean
    public RIAndExpenseUploadRpcService riAndExpenseUploadRpcService() {
        try {
            return new RIAndExpenseUploadRpcService(dbAccessConfig.dsl(),
                    costConfig.accountExpensesStore(),
                    reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                    reservedInstanceConfig.reservedInstanceBoughtStore(),
                    reservedInstanceConfig.reservedInstanceCoverageUpload(),
                    riSupportInPartialCloudEnvironment,
                    costNotificationConfig.costNotificationSender());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create RIAndExpenseUploadRpcService bean", e);
        }
    }
}
