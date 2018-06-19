package org.leesia.mostvalue.schedule;

/**
 * @Auther: leesia
 * @Date: 2018/6/19 21:58
 * @Description:
 */
public enum MostValue {
    MAX("MAX", "最大值"),

    MIN("MIN", "最小值");

    private String value;

    private String desc;

    private MostValue(String value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
