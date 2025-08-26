package com.milesight.beaveriot.mqtt.model;

import com.milesight.beaveriot.context.integration.entity.annotation.Entity;
import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.UniqueConstraint;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import jakarta.persistence.Table;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "sensor_readings",
        uniqueConstraints = {
                @UniqueConstraint(name = "uq_sensor_reading_tenant_eui_fcnt", columnNames = {"tenant_id", "dev_eui", "fcnt"})
        }
)
@Data
public class SensorReading {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "dev_eui", nullable = false)
    private String devEui;

    @Column(name = "gw_eui")
    private String gwEui;

    @Column(name = "app_eui")
    private String appEui;

    @Column(name = "dev_addr")
    private String devAddr;

    @Column(name = "fcnt")
    private Long fcnt;

    private Double temperature;
    private Double humidity;
    private Double battery;

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;
}

