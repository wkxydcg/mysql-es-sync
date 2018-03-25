package com.wosai.mysql.es.sync.bo;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author created by wkx
 * @date 2018/3/5
 **/
@Data
@Accessors(chain = true)
public class ElasticSearchConfig {

    private String url;

    private String username;

    private String password;

}
