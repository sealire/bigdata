package org.leesia.dataio.business.writer;

import org.leesia.dataio.domain.IntData;
import org.leesia.dataio.service.IntDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 08:57
 * @Description: 整数写入
 */
@Component
public class IntDataWriter extends DataWriter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    //要生成的整数个数
    private static final int COUNT = 100_000_000;

    //批量生成的整数个数
    private static final int BATCH_COUNT = 10_000;

    //存储线程睡眠时间
    private static final long SLEEP = 30;

    @Autowired
    private IntDataService dataService;

    private int threadSize = 1;

    private ExecutorService executor;

    public IntDataWriter() {
        threadSize = Runtime.getRuntime().availableProcessors();
        executor = Executors.newFixedThreadPool(threadSize);
    }

    @Override
    public void createAndWrite() {
        List<Integer> sizes = allocateTask();
        long sleep = SLEEP / threadSize;
        for (Integer size : sizes) {
            executor.execute(createTask(size));
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                logger.warn("InterruptedException异常：" + e);
            }
        }
        executor.shutdown();
    }

    @Override
    public void batchCreateAndWrite() {
        List<Integer> sizes = allocateTask();
        long sleep = SLEEP / threadSize;
        for (Integer size : sizes) {
            executor.execute(createBatchTask(size));
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                logger.warn("InterruptedException异常：" + e);
            }
        }
        executor.shutdown();
    }

    private IntData createData() {
        IntData data = new IntData();
        data.setNumber((int) (Math.random() * Integer.MAX_VALUE));
        return data;
    }

    public List<Integer> allocateTask() {
        List<Integer> tasks = new ArrayList<>();
        int size = COUNT / threadSize;
        for (int i = 0; i < threadSize; i++) {
            tasks.add(size);
        }
        if (COUNT % threadSize != 0) {
            int remain = COUNT % threadSize;
            for (int i = 0; i < remain; i++) {
                Integer count = tasks.get(i);
                count++;
                tasks.set(i, count++);
            }
        }
        return tasks;
    }

    private Runnable createTask(int size) {
        return () -> {
            long sleep = SLEEP;
            long start = 0;
            long end = 0;
            boolean exception = false;
            for (int i = 0; i < size; i++) {
                exception = false;
                start = System.currentTimeMillis();
                IntData data = createData();
                try {
                    dataService.insert(data);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    exception = true;
                    end = System.currentTimeMillis();
                    i--;
                    sleep += ((long) (sleep * 0.1));
                    logger.warn("整数入库异常：",  e);
                }
                try {
                    if (exception) {
                        sleep = 5 * (end - start);
                    } else {
                        sleep = (threadSize - 1) * (end - start);
                    }
                    logger.info("sleep：" + sleep + "ms");
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    logger.warn("InterruptedException异常：{}", e);
                }
            }
        };
    }

    private Runnable createBatchTask(final int size) {
        return () -> {
            int total = size;
            while (total > BATCH_COUNT) {
                List<IntData> datas = new ArrayList<>();
                for (int i = 0; i < BATCH_COUNT; i++) {
                    datas.add(createData());
                }
                batchInsert(datas);
                total -= BATCH_COUNT;
            }
            if (total > 0) {
                List<IntData> datas = new ArrayList<>();
                for (int i = 0; i < total; i++) {
                    datas.add(createData());
                }
                batchInsert(datas);
            }
        };
    }

    private synchronized void batchInsert(List<IntData> datas) {
        logger.warn("整数批量入库：",  datas.size());
        dataService.batchInsert(datas);
    }
}
