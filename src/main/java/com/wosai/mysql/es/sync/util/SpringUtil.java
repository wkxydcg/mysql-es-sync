package com.wosai.mysql.es.sync.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author created by wkx
 * @date 2018/3/6
 **/
public class SpringUtil implements ApplicationContextAware {

    private static ConfigurableApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context=(ConfigurableApplicationContext) applicationContext;
    }

    public static void close(){
        context.close();
    }
}
