package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;
    private final JavaMailSender mailSender;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate, JavaMailSender mailSender) {
        this.jdbcTemplate = jdbcTemplate;
        this.mailSender = mailSender;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            String countQuery = "SELECT COUNT(*) FROM products";
            Integer productCount = jdbcTemplate.queryForObject(countQuery, Integer.class);
            jdbcTemplate.query("SELECT productId, productSku, productName, productAmount, productData FROM products LIMIT 5",
            (rs, row) -> new Product(
                rs.getLong("productId"),
                rs.getLong("productSku"),
                rs.getString("productName"),
                rs.getLong("productAmount"),
                rs.getString("productData")
            )).forEach(product -> log.info("Found <{}> in the database.", product.productName()));
            log.info("Total products in database: {}", productCount);

//             sendEmailNotification(jobExecution, productCount);

        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("!!! JOB FAILED! Job ID: {}, Errors: {}",
                jobExecution.getId(), jobExecution.getAllFailureExceptions());
        }
    }

    private void sendEmailNotification(JobExecution jobExecution, Integer count) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo("admin@example.com");
        message.setSubject("Batch Job Completed");
        message.setText(String.format(
            "Job '%s' completed!\nProcessed products: %d",
            jobExecution.getJobInstance().getJobName(),
            count
        ));
        mailSender.send(message);
        log.info("Email notification sent");
    }
}