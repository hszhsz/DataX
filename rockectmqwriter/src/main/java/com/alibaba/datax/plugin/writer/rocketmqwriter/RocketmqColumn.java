package com.alibaba.datax.plugin.writer.rocketmqwriter;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/4/29 14:28
 **/
public class RocketmqColumn {
    String name;

    String type;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
