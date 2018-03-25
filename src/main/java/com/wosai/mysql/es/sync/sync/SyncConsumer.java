package com.wosai.mysql.es.sync.sync;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.wosai.mysql.es.sync.bo.DbConfig;
import com.wosai.mysql.es.sync.bo.ElasticSearchConfig;
import com.wosai.mysql.es.sync.bo.SyncMessage;
import com.wosai.mysql.es.sync.bo.TableConfig;
import com.wosai.mysql.es.sync.constant.ConstantUtil;
import com.wosai.mysql.es.sync.util.BinLogInfoUtil;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

public class SyncConsumer implements Runnable{

    private DbConfig dbConfig;

    private ElasticSearchConfig elasticSearchConfig;

    private Map<String,Map<String,Integer>> columnPosConfig;

    private ArrayBlockingQueue<SyncMessage> queue;

    private String basicAuthorization;

    private OkHttpClient httpClient;

    private String binLogDataPath;

    private static final Logger LOGGER= LoggerFactory.getLogger(SyncConsumer.class);

    SyncConsumer(DbConfig dbConfig, ElasticSearchConfig elasticSearchConfig, Map<String, Map<String, Integer>> columnPosConfig,
                 ArrayBlockingQueue<SyncMessage> queue){
        this.dbConfig=dbConfig;
        this.elasticSearchConfig=elasticSearchConfig;
        this.columnPosConfig=columnPosConfig;
        this.queue=queue;
    }

    @Override
    public void run() {
        initConsumer();
        while (true){
            List<SyncMessage> messageList=new ArrayList<>();
            SyncMessage message;
            while ((message=queue.poll())!=null&&messageList.size()<2000){
                messageList.add(message);
            }
            if(messageList.size()==0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(),e);
                }
            }else{
                handleSyncMessage(messageList);
                updatePos(messageList.get(messageList.size()-1).getEventHeader());
            }
        }
    }

    private void initConsumer(){
        httpClient=new OkHttpClient();
        binLogDataPath = binLogDataPath +":" + dbConfig.getHost() +":"+ dbConfig.getPort()+":"+ dbConfig.getSchema() +".binLogInfo";
//        posPlaceHolder="binLogFile:"+dbConfig.getHost() +":"+ dbConfig.getPort()+":"+dbConfig.getSchema()+":position";
        basicAuthorization=Credentials.basic(elasticSearchConfig.getUsername(), elasticSearchConfig.getPassword());
    }

    private void handleSyncMessage(List<SyncMessage> messageList){
        StringBuilder sb=new StringBuilder();
        for (SyncMessage message:messageList){
            if(message.getEventData() instanceof UpdateRowsEventData){
                sb.append(handleBulkUpdateEventData((UpdateRowsEventData) message.getEventData(),message.getTableConfig())).append("\n");
            }else if(message.getEventData() instanceof WriteRowsEventData){
                sb.append(handleBulkWriteEventData((WriteRowsEventData) message.getEventData(),message.getTableConfig())).append("\n");
            }else if(message.getEventData() instanceof DeleteRowsEventData){
                sb.append(handleBulkDeleteEventData((DeleteRowsEventData) message.getEventData(),message.getTableConfig())).append("\n");
            }
        }
        esBulkRequest(sb.toString());
    }

    private String handleBulkUpdateEventData(UpdateRowsEventData eventData,TableConfig config){
        return eventData.getRows().stream().map(Map.Entry::getValue).map(JSONObject::toJSONString)
                .map(JSONObject::parseArray).map((JSONArray array) -> buildEsIndexBulkStr(array,config))
                .collect(Collectors.joining("\n"));
    }

    private String handleBulkWriteEventData(WriteRowsEventData eventData,TableConfig config){
        return eventData.getRows().stream().map(JSONObject::toJSONString)
                .map(JSONObject::parseArray).map((JSONArray array) -> buildEsIndexBulkStr(array,config))
                .collect(Collectors.joining("\n"));
    }

    private String handleBulkDeleteEventData(DeleteRowsEventData eventData,TableConfig config){
        return eventData.getRows().stream().map(JSONObject::toJSONString)
                .map(JSONObject::parseArray).map((JSONArray array) -> buildEsDeleteBulkStr(array,config))
                .collect(Collectors.joining("\n"));
    }

    private String buildEsIndexBulkStr(JSONArray array,TableConfig config){
        Map<String,Integer> columnPosMap=columnPosConfig.get(config.getName());
        int primaryPos=columnPosMap.get(config.getPrimaryKey())-1;
        StringBuilder sb=new StringBuilder();
        JSONObject actionJson = new JSONObject();
        JSONObject headerJson=new JSONObject();
        JSONObject bodyJson = new JSONObject();
        headerJson.put("_index", config.getEsIndex());
        headerJson.put("_type", config.getEsType());
        Object primaryValue = array.get(primaryPos);
        if(primaryValue==null&& StringUtils.isBlank(String.valueOf(primaryValue))){
            return null;
        }
        headerJson.put("_id", primaryValue);
        for (String column : config.getColumns()) {
            bodyJson.put(column, array.get(columnPosMap.get(column) - 1));
        }
        actionJson.put("index", headerJson);
        sb.append(actionJson.toJSONString()).append("\n");
        sb.append(bodyJson.toJSONString());
        return sb.toString();
    }

    private String buildEsDeleteBulkStr(JSONArray array,TableConfig config){
        Map<String,Integer> columnPosMap=columnPosConfig.get(config.getName());
        int primaryPos=columnPosMap.get(config.getPrimaryKey())-1;
        StringBuilder sb=new StringBuilder();
        JSONObject actionJson = new JSONObject();
        JSONObject headerJson=new JSONObject();
        headerJson.put("_index", config.getEsIndex());
        headerJson.put("_type", config.getEsType());
        Object primaryValue = array.get(primaryPos);
        if(primaryValue==null&& StringUtils.isBlank(String.valueOf(primaryValue))){
            return null;
        }
        headerJson.put("_id", primaryValue);
        actionJson.put("delete", headerJson);
        sb.append(actionJson.toJSONString());
        return sb.toString();
    }

    private void esBulkRequest(String requestBodyStr) {
        String url = elasticSearchConfig.getUrl() + "/_bulk?pretty";
        httpRequest(url, requestBodyStr,true);
    }

    @SuppressWarnings("unchecked")
    private void httpRequest(String url, String requestBodyStr ,boolean bulk) {
        LOGGER.info("url:"+url);
        LOGGER.info("requestBodyStr:"+requestBodyStr);
        RequestBody requestBody = RequestBody.create(ConstantUtil.JSON, requestBodyStr);
        Request request = new Request.Builder().url(url).put(requestBody).header("Authorization",basicAuthorization).build();
        Response response;
        String responseBody = null;
        boolean isNetErrors=false;
        do {
            try {
                response = httpClient.newCall(request).execute();
                if (response.isSuccessful() && response.body() != null) {
                    responseBody=response.body().string();
                    JSONObject responseJson = JSONObject.parseObject(responseBody);
                    if(bulk){
                        if (!responseJson.getBoolean("errors")){
                            return;
                        }
                    }else {
                        return;
                    }
                }
                LOGGER.error("Es insert error:request:"+requestBodyStr+" response:"+responseBody);
            } catch (IOException e) {
                //进行错误通知
                LOGGER.error(e.getMessage() + " request:" + requestBodyStr+" response:"+ responseBody, e);
                isNetErrors=true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(),e);
            }
        }while (isNetErrors);
    }

    @SuppressWarnings("unchecked")
    private void updatePos(EventHeader header){
        JSONObject json=JSONObject.parseObject(JSONObject.toJSONString(header));
        if(json.get(ConstantUtil.NEXT_POSITION)!=null){
            Long nextPosition=json.getLongValue(ConstantUtil.NEXT_POSITION);
            BinLogInfoUtil.updateBinLogPos(binLogDataPath, nextPosition);
        }
    }

}
