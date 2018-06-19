package org.leesia.mostvalue.schedule;

import org.leesia.entity.IntData;
import org.leesia.mostvalue.classify.Classifier;
import org.leesia.mostvalue.reader.IntDataReader;
import org.leesia.mostvalue.util.LeesiaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 20:52
 * @Description: 最值查找调度器
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

    //最值类型
    private MostValue mv = MostValue.MAX;

    //最值个数
    private int count = 10;

    //整数队列
    private BlockingQueue<IntData> blockingQueue;

    public Schedule(int total, int singleBatchCount, int batchCount, MostValue mv, int count) {
        if (total > 0) {
            this.total = total;
        }
        if (singleBatchCount > 0) {
            this.singleBatchCount = singleBatchCount;
        }
        if (batchCount > 0) {
            this.batchCount = batchCount;
        }
        if (mv != null) {
            this.mv = mv;
        }
        if (count > 0) {
            this.count = count;
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
        if (count <= singleBatchCount) {
            //可一次读进内存
            result = readAndSort(from, to, limits.get(0), limits.get(1), count);
            return result;
        }

        int sort_mask = 0;
        int mask = 0b11111111_00000000_00000000_00000000;//排序掩码，从高8位开始桶排序
        List<Integer> paths = new ArrayList<>();//桶排序路径
        List<Bucket> buckets = new ArrayList<>();//桶
        List<Integer> indexes = new ArrayList<>();//最值所在桶编号
        do {
            sort_mask |= mask;
            logger.info("桶排序：total：{}，from：{}，to：{}，min：{}，max：{}，mask：{}", count, from, to, limits.get(0), limits.get(1), LeesiaUtil.toBinaryString(mask));
            //桶排序
            buckets = classify(from, to, limits.get(0), limits.get(1), mask);//分类
            indexes = computeIndex(buckets);//计算最值所在桶编号
            limits = computeLimitAndPath(buckets, indexes, mask, paths);//计算最值所在桶的边界值，即重新计算min，max，并设置paths
            count = 0;//重置count
            for (Integer index : indexes) {
                count += buckets.get(index).getSize();
            }
            mask >>>= 8;
        } while (mask != 0 && count > singleBatchCount);

        if (mask == 0) {
            //最值集中的桶中，paths列表中的数据即为最值
            result.addAll(paths);
        } else {
            result = readAndSort(from, to, limits.get(0), limits.get(1), count);//内存排序
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
     * @param total
     * @return
     */
    private List<Integer> readAndSort(int from, int to, Integer min, Integer max, int total) {
        logger.info("内存排序：total：{}，from：{}，to：{}，min：{}，max：{}", total, from, to, min, max);
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

        if (MostValue.MAX == mv) {
            int count = 0;
            for (int i = datas.size() - 1; i >= 0 && count < this.count; i--) {
                result.add(datas.get(i));
                count++;
            }
        } else if (MostValue.MIN == mv) {
            int count = 0;
            for (int i = 0; i < datas.size() && count < this.count; i++) {
                result.add(datas.get(i));
                count++;
            }
        }

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
     * 计算最值所在桶下标位置
     * @param buckets
     * @return
     */
    private List<Integer> computeIndex(List<Bucket> buckets) {
        logger.info("计算最值所在桶下标位置");
        List<Integer> indexes = new ArrayList<>();

        if (MostValue.MAX == mv) {
            int count = 0;
            for (int i = buckets.size() - 1; i >= 0; i--) {
                int size = buckets.get(i).getSize();
                if ((size + count) < this.count) {
                    count += size;
                    indexes.add(i);
                    continue;
                } else {
                    indexes.add(i);
                    break;
                }
            }
            Collections.reverse(indexes);
        } else if (MostValue.MIN == mv) {
            int count = 0;
            for (int i = 0; i < buckets.size(); i++) {
                int size = buckets.get(i).getSize();
                if ((size + count) < this.count) {
                    count += size;
                    indexes.add(i);
                    continue;
                } else {
                    indexes.add(i);
                    break;
                }
            }
        }

        return indexes;
    }

    /**
     * 计算最值桶边界值，并设置path
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
        int mask_max = paths.get(paths.size() - 1);

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
        List<Integer> masks = new ArrayList<>();
        if (MostValue.MAX == mv) {
            int count = 0;
            for (int i = indexes.size() - 1; i >= 0; i--) {
                if (count >= this.count) {
                    break;
                }
                int size = buckets.get(indexes.get(i)).getSize();
                if (size == 0) {
                    continue;
                }
                int m = buckets.get(indexes.get(i)).getMask();
                for (int k = 0; k < size; k++) {
                    masks.add(m);
                    count++;
                    if (count >= this.count) {
                        break;
                    }
                }
            }
            Collections.reverse(masks);
        } else if (MostValue.MIN == mv) {
            int count = 0;
            for (int i = 0; i < indexes.size(); i++) {
                if (count >= this.count) {
                    break;
                }
                int size = buckets.get(indexes.get(i)).getSize();
                if (size == 0) {
                    continue;
                }
                int m = buckets.get(indexes.get(i)).getMask();
                for (int k = 0; k < size; k++) {
                    masks.add(m);
                    count++;
                    if (count >= this.count) {
                        break;
                    }
                }
            }
        }
        if ((mask & SIGN) != 0) {
            //高8位桶排序时，符号位取反，还原正负数
            for (int m : masks) {
                paths.add(m ^ SIGN);
            }
        } else {
            for (Integer m : masks) {
                int path = paths.remove(0);
                paths.add(path ^ m);
            }
        }
    }
}
