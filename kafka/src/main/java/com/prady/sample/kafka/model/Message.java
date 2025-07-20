package com.prady.sample.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Pradeep Balakrishnan
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String content;
}
