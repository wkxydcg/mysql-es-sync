package com.wosai.mysql.es.sync.bo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author created by wkx
 * @date 2018/3/5
 **/
@Data
@Accessors(chain = true)
public class DbConfig {

    private String host;

    private Integer port;

    private String schema;

    private Integer serviceId;

    private String username;

    private String password;

    private List<TableConfig> tables;

}
