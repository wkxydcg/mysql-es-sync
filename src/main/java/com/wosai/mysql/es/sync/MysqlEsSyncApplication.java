package com.wosai.mysql.es.sync;

import com.wosai.mysql.es.sync.initializer.ProfilesActiveInitializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MysqlEsSyncApplication {

	public static void main(String[] args) {
		SpringApplication springApplication=new SpringApplication(MysqlEsSyncApplication.class);
		springApplication.addInitializers(new ProfilesActiveInitializer());
		springApplication.run(args);
	}
}
