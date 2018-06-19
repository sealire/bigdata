package org.leesia.median.schedule;

import org.leesia.entity.IntData;
import org.leesia.median.classify.Classifier;
import org.leesia.median.reader.IntDataReader;
import org.leesia.median.util.LeesiaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 20:52
 * @Description: 中位数查找调度器
 */
public class Schedule {

    private static final Logger logger = LoggerFactory.getLogger(Schedule.class);

    //队列结束标志
    public static final IntData POISON_PILL = new IntData();

    private static final int SIGN = 0b10000000_00000000_00000000_00000000;//正负数符号位

    //整数个数
    private int total = 0;

    //单次处理整数个数
    private int singleBatchCount = 1_000_000;

    //批量读取的整数个数
    private int batchCount = 10_000;

    //整数队列
    private BlockingQueue<IntData> blockingQueue;

    public Schedule(int total, int singleBatchCount, int batchCount) {
        if (total > 0) {
            this.total = total;
        }
        if (singleBatchCount > 0) {
            this.singleBatchCount = singleBatchCount;
        }
        if (batchCount > 0) {
            this.batchCount = batchCount;
        }

        blockingQueue = new LinkedBlockingQueue<>(this.singleBatchCount);
    }

    public List<Integer> schedule() {
        logger.info("开始调度");
        List<Integer> result = new ArrayList<>();

        if (total < 0) {
            logger.warn("未调度，数据总数<0");
            return result;
        }

        int from = 1;//数据从1开始编号，IntData表id从1开始自增
        int to = total;//数据总量
        int count = total;//要排序的数据量
        List<Integer> limits = new ArrayList<Integer>(){{
            add(Integer.MIN_VALUE);//下边界
            add(Integer.MAX_VALUE);//上边界
        }};
        List<Integer> locations = computeLocation(from - 1, to - 1);//中位数位置，可能有两个
        if (count < singleBatchCount) {
            //可一次读进内存
            result = readAndSort(from, to, limits.get(0), limits.get(1), locations, count);
            return result;
        }

        int sort_mask = 0;
        int mask = 0b11111111_00000000_00000000_00000000;//排序掩码，从高8位开始桶排序
        List<Integer> paths = new ArrayList<>();//桶排序路径
        List<Bucket> buckets = new ArrayList<>();//桶
        List<Integer> indexes = new ArrayList<>();//中位数所在桶编号
        do {
            sort_mask |= mask;
            logger.info("桶排序：total：{}，from：{}，to：{}，min：{}，max{}，locations：{}，mask：{}", count, from, to, limits.get(0), limits.get(1), locations, LeesiaUtil.toBinaryString(mask));
            //桶排序
            buckets = classify(from, to, limits.get(0), limits.get(1), mask);//分类
            indexes = computeIndex(buckets, locations);//计算中位数所在桶编号，并重新计算中位数下标位置
            limits = computeLimitAndPath(buckets, indexes, mask, paths);//计算中位数所在桶的边界值，即重新计算min，max，并设置paths
            count = 0;//重置count
            for (Integer index : indexes) {
                count += buckets.get(index).getSize();
            }
            mask >>>= 8;
        } while (mask != 0 && count > singleBatchCount);

        if (mask == 0) {
            //中位数集中的桶中，paths列表中的数据即为中位数
            result.addAll(paths);
        } else {
            result = readAndSort(from, to, limits.get(0), limits.get(1), locations, count);//内存排序
        }

        logger.info("调度结束，排序掩码路径：{}", LeesiaUtil.toBinaryString(sort_mask));
        return result;
    }

    /**
     * 计算中位数下标位置
     * @param from
     * @param to
     * @return
     */
    private List<Integer> computeLocation(int from, int to) {
        List<Integer> locations = new ArrayList<>();

        long sum = 0;
        sum += from;
        sum += to;
        locations.add((int) (sum / 2));
        if (sum % 2 == 1) {
            locations.add(locations.get(0) + 1);
        }

        return locations;
    }

    /**
     * 内存排序
     * @param from
     * @param to
     * @param min
     * @param max
     * @param locations
     * @param count
     * @return
     */
    private List<Integer> readAndSort(int from, int to, Integer min, Integer max, List<Integer> locations, int count) {
        logger.info("内存排序：total：{}，from：{}，to：{}，min：{}，max：{}，locations：{}", count, from, to, min, max, locations);
        List<Integer> result = new ArrayList<>();

        IntDataReader reader = new IntDataReader(from, to, min, max, batchCount, blockingQueue, POISON_PILL);
        Thread thread = new Thread(reader);
        thread.start();

        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("等待读取数据异常：{}", e);
        }

        List<Integer> datas = new ArrayList<>();
        while (!blockingQueue.isEmpty()) {
            try {
                IntData data = blockingQueue.take();
                if (data == POISON_PILL) {
                    break;
                }
                datas.add(data.getNumber());
            } catch (InterruptedException e) {
                logger.error("数据出队列异常：{}", e);
            }
        }
        Collections.sort(datas);

        locations.forEach(location -> result.add(datas.get(location)));

        return result;
    }

    /**
     * 桶排序
     * @param from
     * @param to
     * @param min
     * @param max
     * @param mask
     * @return
     */
    public List<Bucket> classify(int from, int to, Integer min, Integer max, int mask) {
        logger.info("读取数据并开始桶排序：from：{}，to：{}，min：{}，max：{}，mask：{}", from, to, min, max, LeesiaUtil.toBinaryString(mask));
        IntDataReader reader = new IntDataReader(from, to, min, max, batchCount, blockingQueue, POISON_PILL);
        Thread thread = new Thread(reader);
        thread.start();
        Classifier classifier = new Classifier();
        return classifier.classify(blockingQueue, mask, POISON_PILL);
    }

    /**
     * 计算中位数所在桶下标位置，并更新中位数下标位置
     * @param buckets
     * @param locations
     * @return
     */
    private List<Integer> computeIndex(List<Bucket> buckets, List<Integer> locations) {
        logger.info("计算中位数所在桶下标位置：locations：{}", locations);
        List<Integer> indexes = new ArrayList<>();
        int location = locations.get(0);
        int flag_count = 0;
        int count = 0;
        int index = 0;
        while (count < location && index < buckets.size()) {
            count += buckets.get(index).getSize();
            index++;
        }
        if (index > 0) {
            flag_count = count - buckets.get(index - 1).getSize();
        }
        indexes.add(index - 1);
        if (locations.size() == 2) {
            location = locations.get(1);
            if (count < location && index < buckets.size()) {
                while (count < location) {
                    count += buckets.get(index).getSize();
                    index++;
                }
                indexes.add(index - 1);
            }
        }

        //重新计算中位数下标位置
        locations.set(0, locations.get(0) - flag_count);
        if (locations.size() == 2) {
            locations.set(1, locations.get(1) - flag_count);
        }
        return indexes;
    }

    /**
     * 计算中位数桶边界值，并设置path
     * @param buckets
     * @param indexes
     * @param mask
     * @param paths
     * @return
     */
    private List<Integer> computeLimitAndPath(List<Bucket> buckets, List<Integer> indexes, int mask, List<Integer> paths) {
        List<Integer> limits = new ArrayList<>();

        computePath(buckets, indexes, mask, paths);
        int flag = 0b00000000_00000000_00000000_00000001;//移位标志
        int mask_all = 0b11111111_11111111_11111111_11111111;//全1

        int right = 0;//右移位数
        while ((mask & flag) == 0) {
            right++;
            flag <<= 1;
        }
        right = 32 - right;
        mask_all >>>= right;

        int mask_min = paths.get(0);
        int mask_max = paths.get(0);
        if (paths.size() == 2) {
            mask_max = paths.get(1);
        }

        limits.add(mask_min);//下边界
        limits.add(mask_max ^ mask_all);//上边界

        return limits;
    }

    /**
     * 计算path
     * @param buckets
     * @param indexes
     * @param mask
     * @param paths
     */
    private void computePath(List<Bucket> buckets, List<Integer> indexes, int mask, List<Integer> paths) {
        int mask_min = buckets.get(indexes.get(0)).getMask();//下边界桶掩码位
        int mask_max = buckets.get(indexes.get(0)).getMask();//上边界桶掩码位
        if (indexes.size() == 2) {
            mask_max = buckets.get(indexes.get(1)).getMask();//两个中位数在不同的桶中时，上边界
        }
        if ((mask & SIGN) != 0) {
            //高8位桶排序时，符号位取反，还原正负数
            mask_min ^= SIGN;
            mask_max ^= SIGN;
        }
        if ((mask & SIGN) != 0) {
            //高8位桶排序，paths为空
            paths.add(mask_min);
            if (mask_max != mask_min) {
                paths.add(mask_max);
            }
        } else {
            if (paths.size() == 2) {
                int max = paths.remove(1) ^ mask_max;
                int min = paths.remove(0) ^ mask_min;
                paths.add(min);
                paths.add(max);
            } else {
                int path = paths.remove(0);
                paths.add(path ^ mask_min);
                if (mask_min != mask_max) {
                    paths.add(path ^ mask_max);
                }
            }
        }
    }
}
