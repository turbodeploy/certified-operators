-- Introduce new metric types for chassis.

ALTER TYPE metric_type ADD VALUE IF NOT EXISTS 'POWER';
ALTER TYPE metric_type ADD VALUE IF NOT EXISTS 'COOLING';
