-- Create tables for persisting rejected actions.
CREATE TABLE rejected_actions (
    recommendation_id BIGINT(20) NOT NULL,
    rejected_time TIMESTAMP NOT NULL DEFAULT 0,
    rejected_by VARCHAR(255) NOT NULL,
    rejector_type VARCHAR(255) NOT NULL,

    PRIMARY KEY (recommendation_id)
);

CREATE TABLE rejected_actions_policies (
    recommendation_id BIGINT(20) NOT NULL,
    policy_id BIGINT(20) NOT NULL,

    PRIMARY KEY (recommendation_id, policy_id),
    CONSTRAINT fk_rejected_actions_policies FOREIGN KEY(recommendation_id)
    REFERENCES rejected_actions(recommendation_id) ON
    DELETE CASCADE
);
