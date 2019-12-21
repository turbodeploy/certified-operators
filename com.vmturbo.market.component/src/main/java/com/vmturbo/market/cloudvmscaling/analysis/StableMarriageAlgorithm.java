package com.vmturbo.market.cloudvmscaling.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.market.cloudvmscaling.entities.SMAInput;
import com.vmturbo.market.cloudvmscaling.entities.SMAInputContext;
import com.vmturbo.market.cloudvmscaling.entities.SMAOutput;
import com.vmturbo.market.cloudvmscaling.entities.SMAOutputContext;

/**
 * Stable Marriage Algorithm.
 */

public class StableMarriageAlgorithm {

    /**
     * Given a SMAInput, generate the SMAOutput.
     *
     * @param input the list of input contexts
     * @return the SMA output
     */
    public static SMAOutput execute(@Nonnull SMAInput input) {
        Objects.requireNonNull(input, "StableMarriageAlgorithm.execute() input is null!");
        List<SMAOutputContext> outputContexts = new ArrayList<>();
        for (SMAInputContext inputContext : input.getContexts()) {
            SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
            outputContexts.add(outputContext);
        }
        SMAOutput output = new SMAOutput(outputContexts);
        return output;
    }
}

