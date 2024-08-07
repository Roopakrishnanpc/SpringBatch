package com.Batch.config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// ...
import org.springframework.batch.item.ItemProcessor;

import com.Batch.Model.Industry;
import com.Batch.Model.Industry_db;

public class IdustryItemProcesser implements ItemProcessor<Industry, Industry_db> {
    private static final Logger logger = LoggerFactory.getLogger(IdustryItemProcesser.class);

    @Override
    public Industry_db process(Industry industry) throws Exception {
        try {
            Industry_db industryDb = new Industry_db();
            logger.debug("Processing item: {}", industry);
            // Mapping fields from Industry to Industry_db
            industryDb.setYear(industry.getYear());
            industryDb.setIndustry_aggregation_NZSIOC(industry.getIndustry_aggregation_NZSIOC());
            industryDb.setIndustry_code_NZSIOC(industry.getIndustry_code_NZSIOC());
            industryDb.setIndustry_name_NZSIOC(industry.getIndustry_name_NZSIOC());
            industryDb.setUnits(industry.getUnits());
            industryDb.setVariable_code(industry.getVariable_code());
            industryDb.setVariable_name(industry.getVariable_name());
            industryDb.setVariable_category(industry.getVariable_category());
            industryDb.setValue(industry.getValue());
            industryDb.setIndustry_code_ANZSIC06(industry.getIndustry_code_ANZSIC06());
            return industryDb;
        } catch (Exception e) {
            logger.error("Processing failed for item: {}", industry, e);
            throw new RuntimeException("Processing failed for item", e);
        }
    }
}
