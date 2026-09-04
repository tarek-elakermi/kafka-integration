package com.elakermi.kafka_integration.configs;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {


    @Bean
    public AdminClient adminClient() {
        Properties pros = new Properties();
        pros.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");

        return AdminClient.create(pros);
    }
}
