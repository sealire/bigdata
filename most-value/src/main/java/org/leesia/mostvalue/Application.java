package org.leesia.mostvalue;

import org.apache.commons.lang3.StringUtils;
import org.leesia.dataio.service.IntDataService;
import org.leesia.dataio.service.impl.IntDataServiceImpl;
import org.leesia.mostvalue.schedule.MostValue;
import org.leesia.mostvalue.schedule.Schedule;
import org.leesia.mostvalue.util.LeesiaUtil;
import org.leesia.mostvalue.util.SpringUtil;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.util.List;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 13:44
 * @Description:
 */
@MapperScan("org.leesia.dataio.dao")
@SpringBootApplication
@EnableCaching
@ComponentScan(basePackages={"org.leesia"})
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(Application.class, args);
        SpringUtil.setApplicationContext(ctx);

        int total = 10_000;
        try {
            if (StringUtils.isNotEmpty(args[0])) {
                total = Integer.parseInt(args[0]);
            }
        } catch (Exception e) {
            logger.warn("参数args[0]读取异常");
        }
        int singleBatchCount = 1_000;
        try {
            if (StringUtils.isNotEmpty(args[1])) {
                singleBatchCount = Integer.parseInt(args[1]);
            }
        } catch (Exception e) {
            logger.warn("参数args[1]读取异常");
        }
        int batchCount = 100;
        try {
            if (StringUtils.isNotEmpty(args[2])) {
                batchCount = Integer.parseInt(args[2]);
            }
        } catch (Exception e) {
            logger.warn("参数args[2]读取异常");
        }
        MostValue mv = MostValue.MAX;
        try {
            if (StringUtils.isNotEmpty(args[3])) {
                mv = MostValue.valueOf(args[3]);
            }
        } catch (Exception e) {
            logger.warn("参数args[3]读取异常");
        }
        int count = 10;
        try {
            if (StringUtils.isNotEmpty(args[4])) {
                count = Integer.parseInt(args[4]);
            }
        } catch (Exception e) {
            logger.warn("参数args[4]读取异常");
        }

        long start = System.currentTimeMillis();
        Schedule schedule = new Schedule(total, singleBatchCount, batchCount, mv, count);
        List<Integer> result = schedule.schedule();
        long end = System.currentTimeMillis();
        logger.info("调度结束，调度时间：{}，{}：{}", LeesiaUtil.convertTimeToString(end - start), mv.getDesc(), result);
    }

    @Bean
    public IntDataService intDataService() {
        return new IntDataServiceImpl();
    }
}
