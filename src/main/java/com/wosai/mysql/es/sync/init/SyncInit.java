package com.wosai.mysql.es.sync.init;


import com.alibaba.fastjson.JSONObject;
import com.wosai.mysql.es.sync.bo.DbConfig;
import com.wosai.mysql.es.sync.bo.ElasticSearchConfig;
import com.wosai.mysql.es.sync.bo.TableConfig;
import com.wosai.mysql.es.sync.constant.ConstantUtil;
import com.wosai.mysql.es.sync.sync.SyncProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author created by wkx
 * @date 2018/3/5
 **/
@Component
public class SyncInit implements ApplicationRunner{

    private final Environment environment;

    @Autowired
    public SyncInit(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String configFile=environment.getProperty(ConstantUtil.SYNC_CONFIG_FILE);
        String binLogInfoDataPath = environment.getProperty(ConstantUtil.SYNC_BINLOG_FILE_DATA_FILE_PATH);
        List<String> configFileList= Arrays.asList(configFile.split(","));
        ExecutorService service = Executors.newFixedThreadPool(configFileList.size());
        for (String file:configFileList){
            service.execute(handlerConfigFile(file, binLogInfoDataPath));
        }
    }

    private SyncProducer handlerConfigFile(String configFile, String binLogInfoDataPath) throws IOException {
        Resource fileResource;
        if(configFile.startsWith(File.separator)){
            fileResource=new FileSystemResource(configFile);
        }else{
            fileResource=new ClassPathResource(configFile);
        }
        Yaml yaml = new Yaml();
        JSONObject configJson=yaml.loadAs(fileResource.getInputStream(), JSONObject.class);
        JSONObject dbJson=configJson.getJSONObject(ConstantUtil.DATABASE);
        JSONObject elasticSearchJson=configJson.getJSONObject(ConstantUtil.ELASTICSEARCH);
        DbConfig dbConfig=JSONObject.parseObject(dbJson.toJSONString(),DbConfig.class);
        for (TableConfig config:dbConfig.getTables()){
            assert config.getEsIndex()!=null;
            assert config.getEsType()!=null;
            assert config.getPrimaryKey()!=null;
        }
        ElasticSearchConfig elasticSearchConfig=JSONObject.parseObject(elasticSearchJson.toJSONString(),ElasticSearchConfig.class);
        assert elasticSearchConfig.getUrl()!=null;
        return SyncProducer.builder().dbConfig(dbConfig).elasticSearchConfig(elasticSearchConfig).binLogDatafilePath(binLogInfoDataPath).build();
    }

}
