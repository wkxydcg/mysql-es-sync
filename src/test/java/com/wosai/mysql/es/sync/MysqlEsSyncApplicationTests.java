package com.wosai.mysql.es.sync;

import com.wosai.mysql.es.sync.util.BinLogInfoUtil;
import okhttp3.Credentials;
import org.junit.Test;

import java.io.IOException;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class MysqlEsSyncApplicationTests {

	@Test
	public void contextLoads() throws IOException {
		System.out.println(Credentials.basic("wkx", "123456"));
//		Resource resource=new ClassPathResource("merchant-config-dev.yml");
//		Yaml yaml = new Yaml();
//		JSONObject json=yaml.loadAs(new FileInputStream(resource.getFile()), JSONObject.class);
//		JSONObject dbJson=json.getJSONObject("database");
//		JSONObject elasticsearch=json.getJSONObject("elasticsearch");
//		DbConfig dbConfig=JSONObject.parseObject(dbJson.toJSONString(),DbConfig.class);
//		ElasticSearchConfig elasticSearchConfig=JSONObject.parseObject(elasticsearch.toJSONString(),ElasticSearchConfig.class);
//		System.out.println(dbConfig);
//		System.out.println(elasticSearchConfig);
//		System.out.println(JSONObject.toJSONString(json));

	}

	@Test
	public void testFileWR(){
		String file = "/Users/wkx/dev/project/wkx/java/mysql-es-sync/a";
		//BinLogInfoUtil.updateBinLogFileName(file,"binlog.1123112");
		BinLogInfoUtil.updateBinLogPos(file,13123L);
	}

	@Test
	public void testSTR(){
		String a = ":a";
		System.out.println(a.split(":").length);
		System.out.println(a.split(":")[0]);
	}

}
