package org.leesia.median.reader;

import org.leesia.dataio.service.IntDataService;
import org.leesia.entity.IntData;
import org.leesia.median.util.SpringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @Auther: leesia
 * @Date: 2018/5/23 13:54
 * @Description: 整数读取
 */
public class IntDataReader implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(IntDataReader.class);

    //数据读取
    private IntDataService dataService;

    //起始下标
    private int from;

    //结束下标
    private int to;

    //最小值
    private Integer min;

    //最大值
    private Integer max;

    //批量读取量
    private int batchCount;

    //数据队列
    BlockingQueue<IntData> blockingQueue;

    //队列结束标志
    private IntData POISON_PILL;

    public IntDataReader(int from, int to, Integer min, Integer max, int batchCount, BlockingQueue<IntData> blockingQueue, IntData POISON_PILL) {
        dataService = (IntDataService) SpringUtil.getBean("intDataService");
        this.from = from;
        this.to = to;
        this.min = min;
        this.max = max;
        this.batchCount = batchCount;
        this.blockingQueue = blockingQueue;
        this.POISON_PILL = POISON_PILL;
    }

    @Override
    public void run() {
        logger.info("read data from {} to {}, min：{}，max：{}", from, to, min, max);

        int start = from;
        int end = start + batchCount;
        while (end < to) {
            List<IntData> datas = dataService.bacthRead(start, end - 1, min, max);
            datas.forEach(data -> {
                try {
                    blockingQueue.put(data);
                } catch (InterruptedException e) {
                    logger.error("整数入队列错误，id：{}", data.getId());
                }
            });
            start = end;
            end = start + batchCount;
        }
        if (start < to) {
            List<IntData> datas = dataService.bacthRead(start, to, min, max);
            datas.forEach(data -> {
                try {
                    blockingQueue.put(data);
                } catch (InterruptedException e) {
                    logger.error("整数入队列错误，id：{}", data.getId());
                }
            });
        }

        try {
            blockingQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
            logger.error("结束符入队列错误");
        }

        logger.info("read end from {} to {}, min：{}，max：{}", from, to, min, max);
    }
}
