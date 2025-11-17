package com.example.batchprocessing;

import javax.sql.DataSource;
import java.net.ConnectException;
import java.text.ParseException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfiguration {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public FlatFileItemReader<Product> reader() {
        return new FlatFileItemReaderBuilder<Product>()
            .name("productItemReader")
            .resource(new ClassPathResource("product-data.csv"))
            .delimited()
            .names("productId", "productSku", "productName", "productAmount", "productData")
            .fieldSetMapper(fieldSet -> {
            return new Product(
                fieldSet.readLong("productId"),
                fieldSet.readLong("productSku"),
                fieldSet.readString("productName"),
                fieldSet.readLong("productAmount"),
                fieldSet.readString("productData")
            );
        })
            .build();
    }

    @Bean
    public ProductItemProcessor processor() {
        return new ProductItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Product> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Product>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO products (productId, productSku, productName, productAmount, productData) " +
                 "VALUES (:productId, :productSku, :productName, :productAmount, :productData)" +
                 "ON CONFLICT (productId) DO UPDATE SET " +
                 "productSku = EXCLUDED.productSku, " +
                 "productName = EXCLUDED.productName, " +
                 "productAmount = EXCLUDED.productAmount, " +
                 "productData = EXCLUDED.productData"
                 )
            .dataSource(dataSource)
            .build();
    }

    @Bean
    public Job importProductJob(JobCompletionNotificationListener listener) {
        return new JobBuilder("importProductJob", jobRepository)
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .start(step1())
            .build();
    }

    @Bean
    public Step step1() {
        return new StepBuilder("step1", jobRepository)
            .<Product, Product>chunk(10, transactionManager)
            .reader(reader())
            .processor(processor())
            .writer(writer(null))
            .faultTolerant()
            .retryLimit(3)
            .retry(ConnectException.class)
            .skip(ParseException.class)
            .skipLimit(10)
            .build();
    }
}