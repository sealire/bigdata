package org.leesia.datacreator.writer;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 08:56
 * @Description: 数据写入
 */
public abstract class DataWriter {

    //要生成的整数个数
    protected int total = 100_000_000;

    //批量生成的整数个数
    protected int batchCount = 10_000;

    //线程睡眠时间，ms
    protected long sleep = 10;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public long getSleep() {
        return sleep;
    }

    public void setSleep(long sleep) {
        this.sleep = sleep;
    }

    /**
     * 生成并存储数字
     */
    public abstract void createAndWrite();

    /**
     * 批量生成并存储数字
     */
    public abstract void batchCreateAndWrite();
}
