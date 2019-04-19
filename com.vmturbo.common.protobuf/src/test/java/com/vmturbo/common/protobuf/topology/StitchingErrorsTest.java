package com.vmturbo.common.protobuf.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;

public class StitchingErrorsTest {

    @Test
    public void testEmpty() {
        StitchingErrors errors = new StitchingErrors();
        assertTrue(errors.isNone());
        assertThat(errors, is(StitchingErrors.none()));

        for (StitchingErrorCode code : StitchingErrorCode.values()) {
            assertFalse(errors.contains(code));
        }
    }

    @Test
    public void testFromProtobuf() {
        StitchingErrors errors = new StitchingErrors();
        errors.add(StitchingErrorCode.INCONSISTENT_KEY);

        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(1)
            .setDisplayName("1")
            .setEntityType(10)
            .setPipelineErrors(EntityPipelineErrors.newBuilder()
                .setStitchingErrors(errors.getCode()))
            .build();

        StitchingErrors errorsFromProto = StitchingErrors.fromProtobuf(entity);
        assertThat(errorsFromProto.getCode(), is(errors.getCode()));
        assertTrue(errorsFromProto.contains(StitchingErrorCode.INCONSISTENT_KEY));
    }

    @Test
    public void testAddAndContains() {
        StitchingErrors errors = new StitchingErrors();
        errors.add(StitchingErrorCode.INCONSISTENT_KEY);

        for (StitchingErrorCode code : StitchingErrorCode.values()) {
            if (code == StitchingErrorCode.INCONSISTENT_KEY) {
                assertTrue(errors.contains(code));
            } else {
                assertFalse(errors.contains(code));
            }
        }
    }

    @Test
    public void testMultiContains() {
        StitchingErrors errors = new StitchingErrors();
        errors.add(StitchingErrorCode.INVALID_COMM_BOUGHT);
        errors.add(StitchingErrorCode.INCONSISTENT_KEY);

        assertTrue(errors.contains(StitchingErrorCode.INVALID_COMM_BOUGHT, StitchingErrorCode.INCONSISTENT_KEY));
        assertTrue(errors.contains(StitchingErrorCode.INVALID_COMM_BOUGHT));
        assertTrue(errors.contains(StitchingErrorCode.INCONSISTENT_KEY));

        assertFalse(errors.contains(StitchingErrorCode.INVALID_COMM_BOUGHT,
            StitchingErrorCode.INCONSISTENT_KEY, StitchingErrorCode.INVALID_CONSISTS_OF));
        assertFalse(errors.contains(StitchingErrorCode.INVALID_CONSISTS_OF));
    }

    @Test
    public void testEqualsAndHashCode() {
        final StitchingErrors errors1 = new StitchingErrors();
        errors1.add(StitchingErrorCode.INVALID_COMM_BOUGHT);
        errors1.add(StitchingErrorCode.INCONSISTENT_KEY);

        final StitchingErrors errors2 = new StitchingErrors();
        errors2.add(StitchingErrorCode.INVALID_COMM_BOUGHT);
        errors2.add(StitchingErrorCode.INCONSISTENT_KEY);

        final StitchingErrors diffErrors = new StitchingErrors();
        diffErrors.add(StitchingErrorCode.INVALID_COMM_BOUGHT);

        assertThat(errors1, is(errors2));
        assertThat(errors1, not(diffErrors));
        assertThat(errors1.hashCode(), is(errors2.hashCode()));
    }

    @Test
    public void testToString() {
        final StitchingErrors errors1 = new StitchingErrors();
        errors1.add(StitchingErrorCode.INVALID_COMM_BOUGHT);
        errors1.add(StitchingErrorCode.INCONSISTENT_KEY);

        assertThat(errors1.toString(), is("INVALID_COMM_BOUGHT, INCONSISTENT_KEY"));
    }
}