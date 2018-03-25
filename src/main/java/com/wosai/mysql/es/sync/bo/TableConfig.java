package com.wosai.mysql.es.sync.bo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author created by wkx
 * @date 2018/3/6
 **/
@Data
@Accessors(chain = true)
public class TableConfig {

    private String primaryKey;

    private String name;

    private List<String> columns;

    private String esType;

    private String esIndex;

}
