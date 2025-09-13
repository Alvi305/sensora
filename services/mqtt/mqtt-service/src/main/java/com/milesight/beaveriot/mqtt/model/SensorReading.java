package com.milesight.beaveriot.mqtt.model;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "sensor_readings")
@Data
public class SensorReading {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "dev_eui", nullable = false)
    private String devEui;

    @Column(name = "device_name")
    private String deviceName;


    @Column(name = "gw_eui")
    private String gwEui;

    @Column(name = "app_name")
    private String appName;

    @Column(name = "fcnt")
    private Long fcnt;

    private Double temperature;
    private Double humidity;
    private Double battery;

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;
}

