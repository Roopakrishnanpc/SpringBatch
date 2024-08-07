package com.Batch.config;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.Batch.Model.Industry_db;
import com.Batch.repo.IndustryRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// ...

@Component
public class IndustryItemWriter implements ItemWriter<Industry_db> {

    private static final Logger logger = LoggerFactory.getLogger(IndustryItemWriter.class);

    @Autowired
    private IndustryRepository industryRepository;
    // No-args constructor
    public IndustryItemWriter() {
    }

    // Setter for dependency injection
    public void setIndustryRepository(IndustryRepository industryRepository) {
        this.industryRepository = industryRepository;
    }

    @Override
    public void write(Chunk<? extends Industry_db> chunk) throws Exception {
      //  for (Industry_db item : chunk) {
            try {
                for (Industry_db item : chunk) {
                    // Ensure 'id' is not set here
                	logger.info(item+"is id there?");
                }
            	System.out.println("writing to db" + chunk);
            	logger.info("Loaded to db: writing"+chunk);
                // Write to the database
                industryRepository.saveAll(chunk);
                logger.info("Done to db: writing"+chunk);
            } catch (Exception e) {
                logger.error("Failed to write item: {}, Error: {}", chunk.getItems(), e.getMessage());
                // Optionally, rethrow the exception or handle it as needed
                throw e;
            }
        }
    
}
