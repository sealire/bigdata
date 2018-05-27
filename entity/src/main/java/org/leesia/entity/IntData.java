package org.leesia.entity;

public class IntData extends Data {

    private static final long serialVersionUID = 4680020625121772248L;

    private Integer id;

    private Integer number;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return "{id: " + id  + ", number: " + number + "}";
    }
}