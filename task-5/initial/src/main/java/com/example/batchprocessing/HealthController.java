package com.example.batchprocessing;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {
    
    @GetMapping("/")
    public String home() {
        return "Batch Processing Application is running!";
    }
    
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
}