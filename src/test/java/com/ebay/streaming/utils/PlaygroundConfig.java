package com.ebay.streaming.utils;

import lombok.Data;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;

import java.util.List;

@Data
public class PlaygroundConfig {

    private Integer intVal;
    private Long longVal;
    private String strVal;

    private Boolean boolVal;

    private List<Integer> listVal = Lists.newArrayList();
}
