package com.Batch.partition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
@Component
public class CustomPartition implements Partitioner {
	public Integer count()
	{   String filePath="C:/Users/ASHWIN KRISHNAN/Downloads/example.txt";
		//String filePath ="C:/Users/ASHWIN KRISHNAN/Downloads/annual-enterprise-survey-2023-financial-year-provisional.csv";
			// ("C:/Users/ASHWIN KRISHNAN/Downloads/ItemFileBatch.txt");
	  int rowCount = 0;

    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
        String line;
        // Read and ignore the header line
        if (br.readLine() != null) {
            // Read the remaining lines
            while ((line = br.readLine()) != null) {
                rowCount++;
            }
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
	
    System.out.println("Number of data rows (excluding header): " + rowCount);
    return rowCount;}
//	@Override
//	public Map<String, ExecutionContext> partition(int gridSize) {
//		// TODO Auto-generated method stub
//		int min = 1;
//	    int max = 50990; // Ensure this matches your actual data count
//	    int targetSize = (max - min) / gridSize + 1;
//	    Map<String, ExecutionContext> result = new HashMap<>();
//	    int number = 0;
//	    int start = min;
//	    int end = start + targetSize - 1;
//	    
//	    while (start <= max) {
//	        ExecutionContext value = new ExecutionContext();
//	        // Ensure end does not exceed max
//	        if (end >= max) {
//	            end = max;
//	        }
//	        value.putInt("minValue", start);
//	        value.putInt("maxValue", end);
//	        result.put("partition" + number, value);
//	        start += targetSize;
//	        end += targetSize;
//	        number++;
//	    }
//	    System.out.println("Partitions: " + result);
//	    return result;
//	}
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("partitionNumber", i);
            partitions.put("partition" + i, context);
        }
        return partitions;
    }

}
