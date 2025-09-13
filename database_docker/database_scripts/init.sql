CREATE TABLE IF NOT EXISTS sensor_readings (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    dev_eui TEXT NOT NULL,
    device_name TEXT NOT NULL,
    gw_eui TEXT,
    app_name TEXT,
    fcnt BIGINT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    battery DOUBLE PRECISION,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Prevent duplicates per device counter within a tenant
CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_reading_tenant_eui_fcnt
ON sensor_readings (tenant_id, dev_eui, fcnt)
WHERE fcnt IS NOT NULL;


CREATE INDEX IF NOT EXISTS idx_sensor_readings_tenant_time
ON sensor_readings (tenant_id, received_at);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_dev_eui_time
ON sensor_readings (dev_eui, received_at);