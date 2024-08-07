
package com.Batch.config;

import java.io.File;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
//import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.validation.BindException;

import com.Batch.Model.Industry;
import com.Batch.Model.Industry_db;
import com.Batch.partition.CustomPartition;
import com.Batch.repo.IndustryRepository;

import ch.qos.logback.classic.Logger;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class BatchPartitionConfig {
	@Autowired
	private final JobRepository jobRepository;
	
	//private final StepBuilderFactory stepBuilderFactory;
	@Autowired
	private final PlatformTransactionManager platformTransactionalManager;
	@Autowired
	private final IndustryRepository industryRepository;
//	@Value("classpath:/org/springframework/batch/core/schema")
//	private Resource dropRepositoryTables;
//	@Value("classpath:/org/springframework/batch/core/schema")
//	private Resource dataRepositorySchema;
	@Bean
	public IndustryItemWriter industryItemWriter()
	{
		return new IndustryItemWriter();
	}
	@Bean
	public FlatFileItemReader<Industry> itemReader()
	{
		FlatFileItemReader<Industry> itemReader=new FlatFileItemReader<>();
		itemReader.setName("itemReader");
		itemReader.setResource(new ClassPathResource("annual-enterprise-survey-2023-financial-year-provisional.csv"));//"annual-enterprise-survey-2023-financial-year-provisional.csv"));
	   // FlatFileItemReader<Industry> itemReader = new FlatFileItemReader<>();
//	    File file = new File("C:/Users/ASHWIN KRISHNAN/Downloads/annual-enterprise-survey-2023-financial-year-provisional.csv");
//	    if (!file.exists()) {
//	        throw new RuntimeException("File does not exist: " + file.getAbsolutePath());
//	    }
//	    itemReader.setResource(new FileSystemResource(file));
//		//from which resource we will be raeadig the file
		//name("yourDataReader")
	//itemReader.setResource(new FileSystemResource(classpath:"indestry.csv"));
	//	itemReader.setResource(new FileSystemResource("C:/Users/ASHWIN KRISHNAN/Downloads/annual-enterprise-survey-2023-financial-year-provisional.csv"));
		
	//	itemReader.setResource(new FileSystemResource("C:/Users/ASHWIN KRISHNAN/Downloads/ItemFileBatch.txt"));
	//	itemReader.setResource(new FileSystemResource("C:\\Users\\ASHWIN KRISHNAN\\Downloads\\annual-enterprise-survey-2023-financial-year-provisional.csv"));
		//itemReader.setName("csvReader");
		itemReader.setLinesToSkip(1);
	//	itemReader.setSkipListener(skipListener());
		itemReader.setLineMapper(lineMapper());
	    itemReader.setSkippedLinesCallback(line -> {
	        System.out.println("Skipped line: " + line);
	    });
	    itemReader.open(new ExecutionContext());
	    try {
	        Industry industry;
	        while ((industry = itemReader.read()) != null) {
	            System.out.println("Read item: " + industry);
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    itemReader.close();
	    return itemReader;
	//	return itemReader;
	
	}
	

	private LineMapper<Industry> lineMapper() {
		// TODO Auto-generated method stub
		DefaultLineMapper<Industry> lineMapper=new DefaultLineMapper<Industry>();
		DelimitedLineTokenizer lineTokenizer =new DelimitedLineTokenizer();
		//lineTokenizer.setDelimiter(",");
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("Year", "Industry_aggregation_NZSIOC", "Industry_code_NZSIOC", "Industry_name_NZSIOC", "Units", "Variable_code", "Variable_name", "Variable_category", "Value", "Industry_code_ANZSIC06");

		//object that trnsfer each line to industry object
	    BeanWrapperFieldSetMapper<Industry> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
	    fieldSetMapper.setTargetType(Industry.class);

	    lineMapper.setLineTokenizer(lineTokenizer);
	    lineMapper.setFieldSetMapper(fieldSetMapper);
//		lineMapper.setFieldSetMapper(new BeanWrapperFieldSetMapper<Industry>() {
//		    @Override
//		    public Industry mapFieldSet(FieldSet fieldSet) throws BindException {
//		        Industry industry = super.mapFieldSet(fieldSet);
//		        System.out.println("Mapped Industry: " + industry);
//		        return industry;
//		    }
//		});
		return lineMapper;
	}
	@Bean
	public IdustryItemProcesser processor()
	{
		return new IdustryItemProcesser();
	}
	//this is spring mention since we used partioner I have to actually use save all to store all my menthod at one shot
	@Bean
	public RepositoryItemWriter<Industry_db> writer()
	{
		RepositoryItemWriter<Industry_db> reposioryItemWriter=new RepositoryItemWriter<Industry_db>();
		reposioryItemWriter.setRepository(industryRepository);
		reposioryItemWriter.setMethodName("save");
		return reposioryItemWriter;
	}
	
	//@SuppressWarnings("unchecked")
	

	@Bean
	public Step masterStep(JobRepository jobRepository, PlatformTransactionManager platformtransactionalmanager)
	{
		return new StepBuilder("MasterStep",jobRepository)
				.partitioner(importStep(jobRepository,platformtransactionalmanager).getName(), partitioner())
				.partitionHandler(partitionHandler(jobRepository,platformtransactionalmanager))
				.build();
	}
//	   @Bean
//	    public ItemWriter<Industry_db> writer() {
//	        return new IndustryItemWriter(); // Ensure this class implements ItemWriter
//	    }
	@Bean
	//public Step importStep()
	public Step importStep(JobRepository jobRepository, PlatformTransactionManager platformtransactionalmanager)
	{
//	    return stepBuilderFactory.get("importStep")
//	            .<Industry, Industry_db>chunk(500)
//	            .reader(itemReader())
//	            .processor(processor())
//	            .writer(industryItemWriter)
//	            .taskExecutor(taskExecutor())
//	            .build();
		//System.out.println("Reached here");
		//return new StepBuilderFactory.get("importStep") -> deprecated
		return new StepBuilder("csvImport",jobRepository)
				.<Industry,Industry_db>
				chunk(25495,platformtransactionalmanager)
				.reader(itemReader()).
				processor(processor()).
				//writer(writer()).taskExecutor(taskExecutor()).
				writer(industryItemWriter()).//taskExecutor(taskExecutor()).
				build();
	}
	@Bean
	public CustomPartition partitioner()
	{
		return new CustomPartition();
	}
	@Bean//now we are using grid eg: our data is 51000 then grid size is 50 so 1020 times times each grid execution will happen 
	public PartitionHandler partitionHandler(JobRepository jobRepository, PlatformTransactionManager platformtransactionalmanager)

	{
		
				TaskExecutorPartitionHandler taskExecutorPartitionHandler=new TaskExecutorPartitionHandler();
				taskExecutorPartitionHandler.setGridSize(2);
				taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
				taskExecutorPartitionHandler.setStep(importStep(jobRepository,platformtransactionalmanager));
				return taskExecutorPartitionHandler;
	}
	@Bean
	//public Job runJob()
	public Job runJob(JobRepository jobRepository, PlatformTransactionManager platformtransactionalmanager)
	
	{
		return new JobBuilder("importIndustry", jobRepository).preventRestart()
				.start(masterStep(jobRepository, platformtransactionalmanager))//.next(null) -> for next step
				.build();
	}
    @Bean
    public TaskExecutor taskExecutor() {
    	System.out.println("Reached here taskkexecution");
    	SimpleAsyncTaskExecutor simpleAsyncTaskExecutor =new SimpleAsyncTaskExecutor();
    	simpleAsyncTaskExecutor.setConcurrencyLimit(10); //how much threads to run to improve perfromance
    	return simpleAsyncTaskExecutor;
//    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//    executor.setCorePoolSize(10); // Increase this
//    executor.setMaxPoolSize(20); // Increase this
//    executor.setQueueCapacity(100); // Increase this
//    executor.initialize();
//    return executor;
}
//    @Bean(name="transactionManager")
//    public PlatformTransactionManager getTransactionManager()
//    {
//    	return new ResourcelessTransactionManager();
//    }
//    @Bean(name="jobRepository")
//    public JobRepository getJobRepository() throws Exception
//    {
//    	JobRepositoryFactoryBean factory = new     	JobRepositoryFactoryBean();
//    	factory.setDataSource(mysqlDatasource());
//    	factory.setTransactionManager(getTransactionManager());
//    	factory.afterPropertiesSet();
//    	return factory.getObject();
//    }
//	private DataSource mysqlDatasource() {
//		DriverManagerDataSource datasource=new DriverManagerDataSource();
//		datasource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//		datasource.setUrl("jdbc:mysql://localhost:3306/springframework_batch");
//		datasource.setUsername("root");
//		datasource.setPassword("roopa");
//		dataSourceInitializer(datasource);
//		// TODO Auto-generated method stub
//		return datasource;
//	}
//	private DataSourceInitializer dataSourceInitializer(DataSource datasource) {
//		// TODO Auto-generated method stub
////		ResourceDatabasePopulator databasePopulator=new ResourceDatabasePopulator();
////		databasePopulator.addScript(dropRepositoryTables);
////		databasePopulator.addScript(dataRepositorySchema);
////		databasePopulator.setIgnoreFailedDrops(false);
//		DataSourceInitializer dataSourceInitializer=new DataSourceInitializer();
//		dataSourceInitializer.setDataSource(datasource);
//		//dataSourceInitializer.setDatabasePopulator(databasePopulator);
//		return dataSourceInitializer;
//	}
    
//    FlatFileItemReader: Reads CSV files and maps each line to an Industry object.
//    LineMapper: Uses DelimitedLineTokenizer and BeanWrapperFieldSetMapper to parse and map CSV data to Industry.
//    ItemProcessor: Transforms Industry objects into Industry_db objects.
//    RepositoryItemWriter: Writes Industry_db objects to the database using IndustryRepository.
//    Step: Defines the batch processing step with chunk processing.
//    Job: Defines the batch job that runs the step.
//    TaskExecutor: Configures asynchronous task execution. helps us define how many task execution threads are needed for ou execution
//
    }
