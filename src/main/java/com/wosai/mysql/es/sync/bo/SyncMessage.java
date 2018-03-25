package com.wosai.mysql.es.sync.bo;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@Builder
public class SyncMessage {

    private EventHeader eventHeader;

    private EventData eventData;

    private TableConfig tableConfig;

}
