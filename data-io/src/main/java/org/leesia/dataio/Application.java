package org.leesia.dataio;

import org.leesia.dataio.business.writer.DataWriter;
import org.leesia.dataio.business.writer.IntDataWriter;
import org.leesia.dataio.util.SpringUtil;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@MapperScan("org.leesia.dataio.dao")
@SpringBootApplication
@EnableCaching
@ComponentScan(basePackages={"org.leesia.dataio"})
public class Application {

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(Application.class, args);
        SpringUtil.setApplicationContext(ctx);

        DataWriter dataWriter = (IntDataWriter) SpringUtil.getBean("intDataWriter");
        dataWriter.batchCreateAndWrite();
    }
}