package com.vmturbo.topology.processor.ncm;

import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@link MatrixConfig} implements a Communication Matrix configurator.
 */
@Configuration
public class MatrixConfig {
    @Bean
    public MatrixInterface matrixInterface() {
        return TheMatrix.instance();
    }
}
