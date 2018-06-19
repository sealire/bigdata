package org.leesia.mostvalue.schedule;

/**
 * @Auther: leesia
 * @Date: 2018/5/25 13:41
 * @Description: 桶
 */
public class Bucket {
    
    //桶对应掩码位
    private int mask;

    //桶中整数个数
    private int size;

    public Bucket(int mask, int size) {
        this.mask = mask;
        this.size = size;
    }

    public int getMask() {
        return mask;
    }

    public void setMask(int mask) {
        this.mask = mask;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "{mask: " + mask + ", size: " + size + "}";
    }
}
