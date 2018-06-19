package org.leesia.mostvalue.util;

/**
 * @Auther: leesia
 * @Date: 2018/5/27 13:07
 * @Description: 工具类
 */
public class LeesiaUtil {
    private LeesiaUtil() {
    }

    private final static char[] digits = {'0', '1'};

    /**
     * 转二进制字符串
     * @param num
     * @return
     */
    public static String toBinaryString(int num) {
        char[] buf = new char[32];
        int pos = 32;
        int mask = 1;
        do {
            buf[--pos] = digits[num & mask];
            num >>>= 1;
        } while (pos > 0);

        return new String(buf, pos, 32);
    }

    public static String convertTimeToString(long time) {
        long mill = time % 1000;
        time /= 1000;
        long sec = time % 60;
        time /= 60;
        long min = time % 60;
        time /= 60;
        long hour = time % 24;
        long day = time / 24;

        return  day + "天" + hour + "小时" + min + "分钟" + sec + "秒" + mill + "毫秒";
    }
}
