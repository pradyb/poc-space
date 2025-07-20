package com.prady.sample.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Prady
 */
@Data
@AllArgsConstructor
public class RateDTO {
    private String requestData;
    private String responseData;
}
