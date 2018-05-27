package org.leesia.datacreator;

import org.apache.commons.lang3.StringUtils;
import org.leesia.datacreator.util.SpringUtil;
import org.leesia.datacreator.writer.DataWriter;
import org.leesia.datacreator.writer.IntDataWriter;
import org.leesia.dataio.service.IntDataService;
import org.leesia.dataio.service.impl.IntDataServiceImpl;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@MapperScan("org.leesia.dataio.dao")
@SpringBootApplication
@EnableCaching
@ComponentScan(basePackages={"org.leesia"})
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(Application.class, args);
        SpringUtil.setApplicationContext(ctx);

        int total = 100_000_000;
        try {
            if (StringUtils.isNotEmpty(args[0])) {
                total = Integer.parseInt(args[0]);
            }
        } catch (Exception e) {
            logger.warn("参数args[0]读取异常");
        }
        int batchCount = 10_000;
        try {
            if (StringUtils.isNotEmpty(args[1])) {
                batchCount = Integer.parseInt(args[1]);
            }
        } catch (Exception e) {
            logger.warn("参数args[1]读取异常");
        }
        DataWriter dataWriter = new IntDataWriter(total, batchCount);
        dataWriter.batchCreateAndWrite();
    }

    @Bean
    public IntDataService intDataService() {
        return new IntDataServiceImpl();
    }
}