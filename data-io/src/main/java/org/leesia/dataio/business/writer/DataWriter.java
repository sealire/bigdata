package org.leesia.dataio.business.writer;

/**
 * @Auther: leesia
 * @Date: 2018/5/22 08:56
 * @Description: 数字写入
 */
public abstract class DataWriter {

    /**
     * 生成并存储数字
     */
    public abstract void createAndWrite();

    /**
     * 批量生成并存储数字
     */
    public abstract void batchCreateAndWrite();
}
