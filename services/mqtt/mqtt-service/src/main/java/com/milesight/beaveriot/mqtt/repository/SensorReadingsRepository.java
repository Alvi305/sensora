package com.milesight.beaveriot.mqtt.repository;

import com.milesight.beaveriot.mqtt.model.SensorReading;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface SensorReadingsRepository extends JpaRepository<SensorReading, UUID>
{ }
