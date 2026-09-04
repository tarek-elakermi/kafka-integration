package com.elakermi.kafka_integration.controller;


import com.elakermi.kafka_integration.producer.SimpleProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {


    private final SimpleProducer producer;

    public MessageController() {
        this.producer = new SimpleProducer();
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam String message) {
        producer.sendMessage("topic-1", 1, message);
        return "Message sent: " + message;
    }
}
