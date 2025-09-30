package com.milesight.beaveriot;

import com.milesight.beaveriot.base.cluster.ClusterValidation;
import com.milesight.beaveriot.data.jpa.BaseJpaRepositoryImpl;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.redisson.spring.starter.RedissonAutoConfigurationV2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @author leon
 */
@EnableAsync(proxyTargetClass = true)
@EnableJpaAuditing
@EnableJpaRepositories(repositoryBaseClass = BaseJpaRepositoryImpl.class )
@SpringBootApplication(exclude = {RedissonAutoConfigurationV2.class})
@EnableAspectJAutoProxy(proxyTargetClass = true, exposeProxy = true)
@EnableSchedulerLock(defaultLockAtMostFor = "30s")
@EnableCaching
@ClusterValidation(requiredBeans={RedisConnectionFactory.class})
public class StandardApplication {

    public static void main(String[] args) {
        SpringApplication.run(StandardApplication.class, args);
        System.out.println("javax.net.ssl.trustStore=" + System.getProperty("javax.net.ssl.trustStore"));
        System.out.println("javax.net.ssl.trustStoreType=" + System.getProperty("javax.net.ssl.trustStoreType"));
        System.out.println("javax.net.ssl.keyStore=" + System.getProperty("javax.net.ssl.keyStore"));
    }

}
