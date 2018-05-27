package org.leesia.datacreator.writer;

import org.leesia.datacreator.util.SpringUtil;
import org.leesia.dataio.service.IntDataService;
import org.leesia.entity.IntData;
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
public class IntDataWriter extends DataWriter {

    private static final Logger logger = LoggerFactory.getLogger(IntDataWriter.class);

    private IntDataService dataService;

    private int threadSize = 1;

    private ExecutorService executor;

    public IntDataWriter(int total, int batchCount) {
        dataService = (IntDataService) SpringUtil.getBean("intDataService");
        this.total = total;
        this.batchCount = batchCount;
        threadSize = Runtime.getRuntime().availableProcessors();
        executor = Executors.newFixedThreadPool(threadSize);
    }

    @Override
    public void createAndWrite() {
        List<Integer> sizes = allocateTask();
        for (Integer size : sizes) {
            executor.execute(createTask(size));
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                logger.warn("InterruptedException异常：{}", e);
            }
        }
        executor.shutdown();
    }

    @Override
    public void batchCreateAndWrite() {
        List<Integer> sizes = allocateTask();
        for (Integer size : sizes) {
            executor.execute(createBatchTask(size));
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                logger.warn("InterruptedException异常：{}", e);
            }
        }
        executor.shutdown();
    }

    private IntData createData() {
        IntData data = new IntData();
        int sign = Math.random() > 0.5 ? 1 : -1;
        data.setNumber(sign * (int) (Math.random() * Integer.MAX_VALUE));
        return data;
    }

    public List<Integer> allocateTask() {
        List<Integer> tasks = new ArrayList<>();
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

    private Runnable createTask(int size) {
        return () -> {
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
                    logger.warn("整数入库异常：{}",  e);
                }
                try {
                    if (exception) {
                        sleep = 5 * (end - start);
                    } else {
                        sleep = (threadSize - 1) * (end - start);
                    }
                    logger.info("sleep：{}ms" + sleep);
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
            while (total > batchCount) {
                List<IntData> datas = new ArrayList<>();
                for (int i = 0; i < batchCount; i++) {
                    datas.add(createData());
                }
                batchInsert(datas);
                total -= batchCount;
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
        logger.info("整数批量入库：{}",  datas.size());
        dataService.batchInsert(datas);
    }
}
