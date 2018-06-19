package org.leesia.mostvalue.classify;

import org.leesia.entity.IntData;
import org.leesia.mostvalue.schedule.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @Auther: leesia
 * @Date: 2018/5/24 20:25
 * @Description: 分类器
 */
public class Classifier {

    private static final Logger logger = LoggerFactory.getLogger(Classifier.class);

    //桶映射模式
    private int pattern = 0b00000000_00000000_00000000_11111111;

    //移位标志
    private int flag = 0b00000000_00000000_00000000_00000001;

    //正负数符号标志
    private int sign = 0b10000000_00000000_00000000_00000000;

    public List<Bucket> classify(BlockingQueue<IntData> blockingQueue, int mask, IntData POISON_PILL) {
        //右移位数
        int right = 0;
        while ((mask & flag) == 0) {
            right++;
            flag <<= 1;
        }

        List<Bucket> buckets = createBuckets(right);
        while (true) {
            try {
                IntData data = blockingQueue.take();
                if (data == POISON_PILL) {
                    break;
                }
                int index = mask & data.getNumber();
                if ((mask & sign) != 0) {
                    index ^= sign;//符号位异或，映射到桶
                }
                index >>>= right;//右移到低位
                index &= pattern;//桶映射
                Bucket bucket = buckets.get(index);
                bucket.setSize(bucket.getSize() + 1);
            } catch (InterruptedException e) {
                logger.error("整数出队列错误：{}", e);
            }
        }

        return buckets;
    }

    private List<Bucket> createBuckets(int left) {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            int mask = i << left;
            Bucket bucket = new Bucket(mask, 0);
            buckets.add(bucket);
        }
        return buckets;
    }
}
