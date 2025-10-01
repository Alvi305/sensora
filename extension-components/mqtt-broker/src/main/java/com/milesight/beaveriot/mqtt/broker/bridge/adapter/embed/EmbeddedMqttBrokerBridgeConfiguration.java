package com.milesight.beaveriot.mqtt.broker.bridge.adapter.embed;

import com.milesight.beaveriot.mqtt.broker.bridge.MqttBrokerSettings;
import com.milesight.beaveriot.mqtt.broker.bridge.auth.MqttAuthProvider;
import lombok.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


@Configuration
@ConditionalOnProperty(prefix = "cluster", name = "enabled", havingValue = "false", matchIfMissing = true)
public class EmbeddedMqttBrokerBridgeConfiguration {

    @Bean("embeddedBrokerProperties")
    @ConfigurationProperties(prefix = "mqtt.broker.moquette")
    public Properties embeddedBrokerProperties(MqttBrokerSettings mqttBrokerSettings) {
        val properties = new Properties();

        // Host
        properties.setProperty("host", mqttBrokerSettings.getHost() != null ? mqttBrokerSettings.getHost() : "0.0.0.0");


        properties.setProperty("port", "disabled"); // disable default 1883 clear-text
        if (mqttBrokerSettings.getMqttPort() != null) {
            properties.setProperty("ssl_port", String.valueOf(mqttBrokerSettings.getMqttPort()));
        }


        if (mqttBrokerSettings.getTls() == null || !Boolean.TRUE.equals(mqttBrokerSettings.getTls().getEnabled())) {
            throw new IllegalStateException("TLS-only mode requested: set mqtt.broker.tls.enabled=true and configure keystore.");
        }

        val tls = mqttBrokerSettings.getTls();

        if (tls.getKeyStorePath() == null || tls.getKeyStorePath().isBlank()
                || tls.getKeyStorePassword() == null || tls.getKeyStorePassword().isBlank()) {
            throw new IllegalStateException("Missing TLS keystore configuration: mqtt.broker.tls.keystorePath and keystorePassword are required.");
        }

        // provide SSL properties to Moquette
        properties.setProperty("jks_path", tls.getKeyStorePath());
        properties.setProperty("key_store_password", tls.getKeyStorePassword());
        properties.setProperty("key_manager_password", tls.getKeyStorePassword());
        properties.setProperty("key_store_type", (tls.getKeyStoreType() == null || tls.getKeyStoreType().isBlank()) ? "PKCS12" : tls.getKeyStoreType());

        // mTLS for tighter security
        if (Boolean.TRUE.equals(tls.getClientAuth())) {
            properties.setProperty("need_client_auth", "true");
            if (tls.getTrustStorePath() == null || tls.getTrustStorePath().isBlank() || tls.getTrustStorePassword() == null || tls.getTrustStorePassword().isBlank()) {
                throw new IllegalStateException("mTLS enabled but trust store missing: set mqtt.broker.tls.trustedStorePath and trustedStorePassword.");
            }
            properties.setProperty("trust_store_path", tls.getTrustStorePath());
            properties.setProperty("trust_store_password", tls.getTrustStorePassword());
            properties.setProperty("trust_store_type",
                    (tls.getTrustStoreType() == null || tls.getTrustStoreType().isBlank()) ? properties.getProperty("trust_store_type","PKCS12") : tls.getTrustStoreType());
        }

        // Websockets disabled for TLS-only TCP
        properties.setProperty("websocket_port", "disabled");
        properties.setProperty("secure_websocket_port", "disabled");


        properties.setProperty("allow_anonymous", "false");
        properties.setProperty("persistence_enabled", "false");
        properties.setProperty("netty.mqtt.message_size", "1048576");
        return properties;
    }


    @Bean(name = "embeddedMqttBrokerBridge", initMethod = "open", destroyMethod = "close")
    public EmbeddedMqttBrokerBridge embeddedMqttBrokerBridge(MqttAuthProvider mqttAuthProvider,
                                                              @Qualifier("embeddedBrokerProperties") Properties embeddedBrokerProperties) {
        return new EmbeddedMqttBrokerBridge(mqttAuthProvider, embeddedBrokerProperties);
    }

}
