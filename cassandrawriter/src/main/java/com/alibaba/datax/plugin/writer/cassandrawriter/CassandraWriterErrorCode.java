package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @ClassName CassandraWriterErrorCode
 * @Description CassandraWriter错误代码
 * @Author heshaozhong
 * @Date 上午7:43 2018/8/14
 */
public enum CassandraWriterErrorCode implements ErrorCode {
    INSERT_CASSANDRA_ERROR("CassandraWriter-01", "Insert Cassandra表时发生异常."),
    CREATE_CASSANDRA_ERROR("CassandraWriter-02", "create Cassandra表时发生异常.");

    private final String code;
    private final String description;

    private CassandraWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
