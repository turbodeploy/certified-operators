-- Create tables for persisting accepted actions.
CREATE TABLE accepted_actions (
    recommendation_id BIGINT(20) NOT NULL,
    accepted_time TIMESTAMP NOT NULL DEFAULT 0,
    latest_recommendation_time TIMESTAMP NOT NULL DEFAULT 0,
    accepted_by VARCHAR(255) NOT NULL,
    acceptor_type VARCHAR(255) NOT NULL,

    PRIMARY KEY (recommendation_id)
);

CREATE TABLE accepted_actions_policies (
    recommendation_id BIGINT(20) NOT NULL,
    policy_id BIGINT(20) NOT NULL,

    PRIMARY KEY (recommendation_id, policy_id),
    CONSTRAINT fk_accepted_actions_policies FOREIGN KEY (recommendation_id)
            REFERENCES accepted_actions(recommendation_id) ON DELETE CASCADE
);
