package com.Batch.config;

import java.io.File;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
//import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.Batch.Model.Industry;
import com.Batch.Model.Industry_db;
import com.Batch.partition.CustomPartition;
import com.Batch.repo.IndustryRepository;

import ch.qos.logback.classic.Logger;
import lombok.RequiredArgsConstructor;

//@Configuration
@RequiredArgsConstructor

public class BatchConfig {
	@Autowired
	private final JobRepository jobRepository;
	
	//private final StepBuilderFactory stepBuilderFactory;
	@Autowired
	private final PlatformTransactionManager platformTransactionalManager;
	@Autowired
	private final IndustryRepository industryRepository;
	@Autowired
	private IndustryItemWriter industryItemWriter;
	@Bean
	public FlatFileItemReader<Industry> itemReader()
	{
		FlatFileItemReader<Industry> itemReader=new FlatFileItemReader<>();
	   // FlatFileItemReader<Industry> itemReader = new FlatFileItemReader<>();
//	    File file = new File("C:/Users/ASHWIN KRISHNAN/Downloads/annual-enterprise-survey-2023-financial-year-provisional.csv");
//	    if (!file.exists()) {
//	        throw new RuntimeException("File does not exist: " + file.getAbsolutePath());
//	    }
//	    itemReader.setResource(new FileSystemResource(file));
//		//from which resource we will be raeadig the file
		//name("yourDataReader")
		itemReader.setResource(new FileSystemResource("C:/Users/ASHWIN KRISHNAN/Downloads/example.txt"));
		//itemReader.setResource(new FileSystemResource("C:/Users/ASHWIN KRISHNAN/Downloads/annual-enterprise-survey-2023-financial-year-provisional.csv"));
		
	//	itemReader.setResource(new FileSystemResource("C:/Users/ASHWIN KRISHNAN/Downloads/ItemFileBatch.txt"));
	//	itemReader.setResource(new FileSystemResource("C:\\Users\\ASHWIN KRISHNAN\\Downloads\\annual-enterprise-survey-2023-financial-year-provisional.csv"));
		itemReader.setName("csvReader");
		itemReader.setLinesToSkip(1);
	//	itemReader.setSkipListener(skipListener());
		itemReader.setLineMapper(lineMapper());
		return itemReader;
	
	}
//	@Bean
//	public SkipListener skipListener() {
//		Logger logger = (Logger) LoggerFactory.getLogger(BatchConfig.class);
//	    return new SkipListener() {
//	        @Override
//	        public void onSkipInRead(Throwable t) {
//	            logger.error("Error reading line: {}", t.getMessage());
//	    }
//	    }
//	}

	private LineMapper<Industry> lineMapper() {
		// TODO Auto-generated method stub
		DefaultLineMapper<Industry> lineMapper=new DefaultLineMapper<Industry>();
		DelimitedLineTokenizer lineTokenizer =new DelimitedLineTokenizer();
		//lineTokenizer.setDelimiter(",");
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("Year", "Industry_aggregation_NZSIOC", "Industry_code_NZSIOC", "Industry_name_NZSIOC", "Units", "Variable_code", "Variable_name", "Variable_category", "Value", "Industry_code_ANZSIC06");

		//object that trnsfer each line to industry object
		BeanWrapperFieldSetMapper<Industry> fieldSetWeapper=new BeanWrapperFieldSetMapper<>();
		fieldSetWeapper.setTargetType(Industry.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetWeapper);
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
	

//	@Bean
//	public Step masterStep()	{
//		return new StepBuilder("MasterStep",jobRepository)
//				.partitioner(importStep().getName(), partitioner())
//				.partitionHandler(partitionHandler())
//				.build();
//	}
	@Bean
	//public Step importStep()
	public Step importStep()	{
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
				chunk(1020,platformTransactionalManager)
				.reader(itemReader()).
				processor(processor()).
				writer(writer()).taskExecutor(taskExecutor()).
				//writer(industryItemWriter).//taskExecutor(taskExecutor()).
				build();
	}
	@Bean
	//public Job runJob()
	public Job runJob()
	
	{
		return new JobBuilder("importIndustry", jobRepository)
				.start(importStep())//.next(null) -> for next step
				.build();
	}
    @Bean
    public TaskExecutor taskExecutor() {
    	System.out.println("Reached here taskkexecution");
    	SimpleAsyncTaskExecutor simpleAsyncTaskExecutor =new SimpleAsyncTaskExecutor();
    	simpleAsyncTaskExecutor.setConcurrencyLimit(10); //how much threads to run to improve perfromance
    	return simpleAsyncTaskExecutor;
    	//the above is basic and we can use that as well
//        return new ThreadPoolTaskExecutor() {{
//            setCorePoolSize(10);
//            setMaxPoolSize(20);
//            setQueueCapacity(50);
//            initialize();
//        }};
    	
    }
//    FlatFileItemReader: Reads CSV files and maps each line to an Industry object.
//    LineMapper: Uses DelimitedLineTokenizer and BeanWrapperFieldSetMapper to parse and map CSV data to Industry.
//    ItemProcessor: Transforms Industry objects into Industry_db objects.
//    RepositoryItemWriter: Writes Industry_db objects to the database using IndustryRepository.
//    Step: Defines the batch processing step with chunk processing.
//    Job: Defines the batch job that runs the step.
//    TaskExecutor: Configures asynchronous task execution. helps us define how many task execution threads are needed for ou execution
//
}
