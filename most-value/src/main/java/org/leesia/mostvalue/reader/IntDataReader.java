package org.leesia.mostvalue.reader;

import org.leesia.dataio.service.IntDataService;
import org.leesia.entity.IntData;
import org.leesia.mostvalue.util.SpringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

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

    //读线程个数
    private int threadSize = 1;

    public IntDataReader(int from, int to, Integer min, Integer max, int batchCount, BlockingQueue<IntData> blockingQueue, IntData POISON_PILL) {
        dataService = (IntDataService) SpringUtil.getBean("intDataService");
        this.from = from;
        this.to = to;
        this.min = min;
        this.max = max;
        this.batchCount = batchCount;
        this.blockingQueue = blockingQueue;
        this.POISON_PILL = POISON_PILL;
        threadSize = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public void run() {
        logger.info("read data from {} to {}, min：{}，max：{}", from, to, min, max);
        CountDownLatch countDownLatch = new CountDownLatch(threadSize);
        List<DataReader> readers = allocateTask(countDownLatch);
        for (DataReader reader : readers) {
            Thread thread = new Thread(reader);
            thread.start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("等待读任务结束错误：{}", e);
        }

        try {
            blockingQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
            logger.error("结束符入队列错误：{}", e);
        }

        logger.info("read end from {} to {}, min：{}，max：{}", from, to, min, max);
    }

    private List<DataReader> allocateTask(CountDownLatch countDownLatch) {
        List<DataReader> readers = new ArrayList<>();
        List<Integer> tasks = allocateTask();
        logger.info("分配读任务：{}", tasks);
        int start = from;
        for (Integer task : tasks) {
            DataReader reader = new DataReader(start, start + task - 1, countDownLatch);
            readers.add(reader);
            start += task;
        }
        
        return readers;
    }

    public List<Integer> allocateTask() {
        List<Integer> tasks = new ArrayList<>();
        int total = to - from + 1;
        int size = total / threadSize;
        for (int i = 0; i < threadSize; i++) {
            tasks.add(size);
        }
        if (total % threadSize != 0) {
            int remain = total % threadSize;
            for (int i = 0; i < remain; i++) {
                Integer count = tasks.get(i);
                count++;
                tasks.set(i, count++);
            }
        }
        return tasks;
    }

    class DataReader implements Runnable {

        //起始下标
        private int from;

        //结束下标
        private int to;

        private CountDownLatch countDownLatch;

        public DataReader(int from, int to, CountDownLatch countDownLatch) {
            this.from = from;
            this.to = to;
            this.countDownLatch = countDownLatch;
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

            countDownLatch.countDown();
        }
    }
}
