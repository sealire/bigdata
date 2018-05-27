package org.leesia.median.classify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.leesia.entity.IntData;
import org.leesia.median.schedule.Bucket;
import org.leesia.median.schedule.Schedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Auther: leesia
 * @Date: 2018/5/24 20:52
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ClassifierTest {

    @Test
    public void testClassifier() {
        BlockingQueue<IntData> blockingQueue = new LinkedBlockingQueue<>(100);
        List<IntData> datas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int sign = Math.random() > 0.5 ? 1 : -1;
            IntData data = new IntData();
            data.setId(i);
            data.setNumber(sign * (int) (Math.random() * Integer.MAX_VALUE));
            datas.add(data);
        }
        System.out.println(datas);
        for (IntData data : datas) {
            try {
                blockingQueue.put(data);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            blockingQueue.put(Schedule.POISON_PILL);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Classifier classifier = new Classifier();

        int mask = 0b11111111_00000000_00000000_00000000;
        List<Bucket> bucket = classifier.classify(blockingQueue, mask, Schedule.POISON_PILL);
        System.out.println(bucket);
    }
}
