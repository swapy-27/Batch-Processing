package examples.batchprocessing.config;


import examples.batchprocessing.entities.Customer;

import examples.batchprocessing.partitioning.ColumnRangePartitioner;
import examples.batchprocessing.repositories.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;

import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {
// these has been deprecated
//    @Autowired
//    private JobBuilderFactory jobBuilderFactory;


//    @Autowired
//    private StepBuilderFactory stepBuilderFactory;
//
    @Autowired
    private CustomerRepository customerRepository;


    //ItemReader

    @Bean
    public FlatFileItemReader<Customer> reader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }

    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        return null;
    }
    @Bean
    public CustomerWriter customerWriter(){
        return new CustomerWriter();
    }
    // we are parsing not in single rows now
//    @Bean
//    public RepositoryItemWriter<Customer> writer() {
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }

    //    @Bean
//    public Step step1() {
//        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
//                .reader(reader())
//                .processor(processor())
//                .writer(writer())
//                .taskExecutor(taskExecutor())
//                .build();
//    }
//
//    @Bean
//    public Job runJob() {
//        return jobBuilderFactory.get("importCustomers")
//                .flow(step1()).end().build();
//
//    }
    @Bean
    public ColumnRangePartitioner columnRangePartition(){
        return new ColumnRangePartitioner();
    }

    @Bean
    public PartitionHandler partitionHandler(JobRepository jobRepository,PlatformTransactionManager platformTransactionManager){
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();

        taskExecutorPartitionHandler.setGridSize(4);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep(jobRepository,platformTransactionManager));

        return taskExecutorPartitionHandler;
    }

    public Step slaveStep( JobRepository jobRepository,PlatformTransactionManager platformTransactionManager){
        return new StepBuilder("slaveStep",jobRepository).<Customer, Customer>chunk(10,platformTransactionManager)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter())
                .build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository,PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("masterStep",jobRepository)
                .partitioner(slaveStep(jobRepository,platformTransactionManager).getName(),columnRangePartition())
                .partitionHandler(partitionHandler(jobRepository,platformTransactionManager))
                .build();
    }

    @Bean
    public Job runJob(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new JobBuilder("importCustomers",jobRepository)
                .flow(masterStep(jobRepository,platformTransactionManager)).end().build();

    }


    // we don't want 20 threads for this scenario
//    @Bean
//    public TaskExecutor taskExecutor() {
//        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
//        asyncTaskExecutor.setConcurrencyLimit(20);
//        return asyncTaskExecutor;
//    }

    @Bean
    public TaskExecutor taskExecutor() {

        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        threadPoolTaskExecutor.setMaxPoolSize(4);
        threadPoolTaskExecutor.setCorePoolSize(4);
        threadPoolTaskExecutor.setQueueCapacity(4);

        return threadPoolTaskExecutor;
    }


}
