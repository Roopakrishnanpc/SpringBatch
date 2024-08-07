package com.Batch.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
//@Entity
public class Industry {
//	@Id
//	@GeneratedValue(strategy = GenerationType.AUTO)
//	private Integer id;
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
