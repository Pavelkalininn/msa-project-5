package com.example.batchprocessing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job importProductJob;

    @GetMapping("/")
    public String home() {
        return "Batch Processing Application is running! Use POST /start to launch job";
    }

    @GetMapping("/health")
    public String health() {
        return "OK";
    }

    @GetMapping("/start")
    public String startJob() {
        try {
            JobParameters parameters = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

            jobLauncher.run(importProductJob, parameters);
            return "Batch job started successfully!";
        } catch (Exception e) {
            return "Error starting job: " + e.getMessage();
        }
    }
}