package com.wosai.mysql.es.sync.initializer;

import com.wosai.mysql.es.sync.constant.ConstantUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author created by wkx
 * @date 2018/3/8
 **/
public class ProfilesActiveInitializer implements ApplicationContextInitializer{

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        String activeFile=System.getenv(ConstantUtil.SPRING_PROFILES_ACTIVE);
        if(StringUtils.isNotEmpty(activeFile)&&applicationContext.getEnvironment().getActiveProfiles().length<=0){
            applicationContext.getEnvironment().addActiveProfile(activeFile);
        }
    }
}
