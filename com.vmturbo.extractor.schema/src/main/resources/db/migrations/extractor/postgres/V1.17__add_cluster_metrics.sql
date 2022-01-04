-- Introduce new metric types for cluster properties.
ALTER TYPE metric_type ADD VALUE 'CPU_HEADROOM';
ALTER TYPE metric_type ADD VALUE 'MEM_HEADROOM';
ALTER TYPE metric_type ADD VALUE 'STORAGE_HEADROOM';
ALTER TYPE metric_type ADD VALUE 'TOTAL_HEADROOM';


