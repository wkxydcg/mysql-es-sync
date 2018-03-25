package com.wosai.mysql.es.sync.sync;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import com.wosai.mysql.es.sync.bo.DbConfig;
import com.wosai.mysql.es.sync.bo.ElasticSearchConfig;
import com.wosai.mysql.es.sync.bo.SyncMessage;
import com.wosai.mysql.es.sync.bo.TableConfig;
import com.wosai.mysql.es.sync.util.BinLogInfoUtil;
import com.wosai.mysql.es.sync.util.JdbcUtil;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author created by wkx
 * @date 2018/3/5
 **/
@Data
@Accessors(chain = true)
@Builder
public class SyncProducer implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncProducer.class);

    private String binLogDatafilePath;

    private String binLogDataPath;

    private DbConfig dbConfig;

    private ElasticSearchConfig elasticSearchConfig;

    private Map<String, Map<String, Integer>> columnPosConfig;

    private ArrayBlockingQueue<SyncMessage> queue;

    @Override
    public void run() {
        try {
            columnPosConfig = JdbcUtil.selectColumnInfo(dbConfig);
            initParam();
            initSyncConsumer();
            initBinLogClient();
        } catch (IOException | SQLException e) {
            LOGGER.error(Thread.currentThread().getId() + ":" + e.getMessage(), e);
            System.exit(0);
        }
    }
    private void initParam(){
        queue=new ArrayBlockingQueue<>(1024);
        binLogDataPath = binLogDataPath +":" + dbConfig.getHost() +":"+ dbConfig.getPort()+":"+ dbConfig.getSchema() +".binLogInfo";
    }

    private void initSyncConsumer(){
        SyncConsumer syncConsumer=new SyncConsumer(dbConfig,elasticSearchConfig,columnPosConfig,queue);
        new Thread(syncConsumer).start();
    }

    @SuppressWarnings("unchecked")
    private void initBinLogClient() throws IOException {
        BinaryLogClient client = new BinaryLogClient(dbConfig.getHost(), dbConfig.getPort(), dbConfig.getSchema(), dbConfig.getUsername(), dbConfig.getPassword());
        client.setServerId(dbConfig.getServiceId());
        final Map<Long, TableMapEventData> tableMap = new HashMap<>();
        final Map<EventType, EventDataDeserializer> des = new HashMap<EventType, EventDataDeserializer>() {{
            put(EventType.ROTATE, new RotateEventDataDeserializer());
            put(EventType.WRITE_ROWS, new WriteRowsEventDataDeserializer(tableMap));
            put(EventType.UPDATE_ROWS, new UpdateRowsEventDataDeserializer(tableMap));
            put(EventType.DELETE_ROWS, new DeleteRowsEventDataDeserializer(tableMap));
            put(EventType.EXT_WRITE_ROWS, new WriteRowsEventDataDeserializer(tableMap).setMayContainExtraInformation(true));
            put(EventType.EXT_UPDATE_ROWS, new UpdateRowsEventDataDeserializer(tableMap).setMayContainExtraInformation(true));
            put(EventType.EXT_DELETE_ROWS, new DeleteRowsEventDataDeserializer(tableMap).setMayContainExtraInformation(true));
            put(EventType.GTID, new GtidEventDataDeserializer());
        }};
        Object binlogName = BinLogInfoUtil.getBinLogFileName(binLogDataPath);
        Object binlogPos = BinLogInfoUtil.getBinLogPos(binLogDataPath);
        if (binlogName != null&& binlogPos!=null) {
            LOGGER.info("binlogName:{}",binlogName);
            LOGGER.info("binlogPos:{}",binlogPos);
            client.setBinlogFilename(String.valueOf(binlogName));
            client.setBinlogPosition(Long.parseLong(String.valueOf(binlogPos)));
        }
        EventDeserializer deserializer = new EventDeserializer(new EventHeaderV4Deserializer(), new NullEventDataDeserializer(), des, tableMap);
        client.setEventDeserializer(deserializer);
        client.registerEventListener(event -> handleEvent(event, tableMap));
        LOGGER.info("client beginListen:{}");
        client.connect();
    }

    private void handleEvent(Event event, Map<Long, TableMapEventData> tableMap) {
        EventHeader header = event.getHeader();
        EventType eventType = header.getEventType();
        EventData eventData = event.getData();
        if (eventType == EventType.ROTATE) {
            RotateEventData data = (RotateEventData) eventData;
            String binlogFilename = data.getBinlogFilename();
            BinLogInfoUtil.updateBinLogFileName(binLogDataPath, binlogFilename);
        } else if (eventType == EventType.UPDATE_ROWS || eventType == EventType.EXT_UPDATE_ROWS) {
            UpdateRowsEventData data = (UpdateRowsEventData) eventData;
            for (TableConfig config : dbConfig.getTables()) {
                if (config.getName().equals(tableMap.get(data.getTableId()).getTable())) {
                    SyncMessage syncMessage=SyncMessage.builder().eventData(eventData).eventHeader(header).tableConfig(config).build();
                    putMessageToSyncQueue(syncMessage);
                    break;
                }
            }
        } else if (eventType == EventType.WRITE_ROWS || eventType == EventType.EXT_WRITE_ROWS) {
            WriteRowsEventData data = (WriteRowsEventData) eventData;
            for (TableConfig config : dbConfig.getTables()) {
                if (config.getName().equals(tableMap.get(data.getTableId()).getTable())) {
                    SyncMessage syncMessage=SyncMessage.builder().eventData(eventData).eventHeader(header).tableConfig(config).build();
                    putMessageToSyncQueue(syncMessage);
                    break;
                }
            }
        }
    }

    private void putMessageToSyncQueue(SyncMessage message){
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
