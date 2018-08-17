package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/16
 */
public enum CassandraReadErrorCode implements ErrorCode {
    //SplitPK 错误

    CASSANDRA_SPLIT_PK_ERROR("CassandraReadErrorCode-11", "SplitPK错误，请检查"),
    CASSANDRA_ALLOW_FILTER_ERROR("CassandraReadErrorCode-11", "allowFilter配置不允许filter，请检查");

    private final String code;

    private final String description;

    private CassandraReadErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
