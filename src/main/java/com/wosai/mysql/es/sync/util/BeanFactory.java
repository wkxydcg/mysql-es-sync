package com.wosai.mysql.es.sync.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author created by wkx
 * @date 2018/3/5
 **/
@Component
public class BeanFactory implements ApplicationContextAware{

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context=applicationContext;
    }

    public static <T> T getBean(Class<T> c){
        return context.getBean(c);
    }

    public static Object getBean(String name){
        return context.getBean(name);
    }
}
