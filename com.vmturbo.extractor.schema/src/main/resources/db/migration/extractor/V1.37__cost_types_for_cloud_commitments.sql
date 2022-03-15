/*
** Add new enum values cloud commitment discount and usage
*/

ALTER TYPE cost_category ADD VALUE IF NOT EXISTS 'CLOUD_COMMITMENT_USAGE';
ALTER TYPE cost_source ADD VALUE IF NOT EXISTS 'CLOUD_COMMITMENT_DISCOUNT';

