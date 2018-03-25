package com.wosai.mysql.es.sync.constant;

import okhttp3.MediaType;

public class ConstantUtil {

    public static final String SPRING_PROFILES_ACTIVE="spring.profiles.active";

    public static final String DATABASE="database";

    public static final String ELASTICSEARCH="elasticSearch";

    public static final String SYNC_CONFIG_FILE="sync.config.file";

    public static final String SYNC_BINLOG_FILE_DATA_FILE_PATH = "sync.binLogInfo.store.filePath";

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public static final String NEXT_POSITION="nextPosition";


}
