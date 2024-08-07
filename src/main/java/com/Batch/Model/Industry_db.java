package com.Batch.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
@Data
@Getter
@Setter
@Entity
public class Industry_db {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
    private String Year;  // Match the CSV header
    private String Industry_aggregation_NZSIOC;  // Corrected property name
    private String Industry_code_NZSIOC;  // Corrected property name
    private String Industry_name_NZSIOC;  // Corrected property name
    private String Units;  // Match the CSV header
    private String Variable_code;  // Match the CSV header
    private String Variable_name;  // Match the CSV header
    private String Variable_category;  // Match the CSV header
    private String Value;  // Match the CSV header
    private String Industry_code_ANZSIC06;  // Match the CSV header
}
