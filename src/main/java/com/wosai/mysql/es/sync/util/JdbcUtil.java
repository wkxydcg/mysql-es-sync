package com.wosai.mysql.es.sync.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.wosai.mysql.es.sync.bo.DbConfig;
import com.wosai.mysql.es.sync.bo.TableConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author created by wkx
 * @date 2018/3/6
 **/
public class JdbcUtil {

    private static Map<String,DataSource> dataSourceMap=new ConcurrentHashMap<>();

    private static final String SELECT_COLUMN_INFO="SELECT COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS WHERE table_name= ?";

    private static final String COLUMN_NAME="COLUMN_NAME";

    private static final String ORDINAL_POSITION="ORDINAL_POSITION";

    public static Map<String,Map<String,Integer>> selectColumnInfo(DbConfig dbConfig) throws SQLException {
        Connection connection=getConnect(dbConfig);
        Map<String,Map<String,Integer>> mapMap=new HashMap<>();
        for (TableConfig config:dbConfig.getTables()){
            PreparedStatement statement=connection.prepareStatement(SELECT_COLUMN_INFO);
            statement.setString(1,config.getName());
            ResultSet resultSet=statement.executeQuery();
            Map<String,Integer> map=new HashMap<>();
            while (resultSet.next()){
                String key=resultSet.getString(COLUMN_NAME);
                int value=resultSet.getInt(ORDINAL_POSITION);
                map.put(key,value);
            }
            mapMap.put(config.getName(),map);
        }
        return mapMap;
    }

    private static Connection getConnect(DbConfig dbConfig) throws SQLException {
        String key=dbConfig.getHost()+dbConfig.getPort()+dbConfig.getSchema();
        if(dataSourceMap.containsKey(key)){
            return dataSourceMap.get(key).getConnection();
        }else{
            synchronized (JdbcUtil.class){
                if(dataSourceMap.containsKey(key)){
                    return dataSourceMap.get(key).getConnection();
                }else{
                    DruidDataSource dataSource=new DruidDataSource();
                    String url="jdbc:mysql://"+dbConfig.getHost()+":"+dbConfig.getPort()+"/"+dbConfig.getSchema();
                    dataSource.setUrl(url);
                    dataSource.setMaxActive(1);
                    dataSource.setUsername(dbConfig.getUsername());
                    dataSource.setPassword(dbConfig.getPassword());
                    Connection connection=dataSource.getConnection();
                    dataSourceMap.put(key,dataSource);
                    return connection;
                }
            }
        }
    }

}
