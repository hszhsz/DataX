package cn.ctyun.datax.plugin.reader.hanareader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * HANA错误编码
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public enum HANAReaderErrorCode implements ErrorCode {
    /***/
    CLASS_NOT_FOUND("CLASS_NOT_FOUND","jar包无法找到com.sap.db.jdbc.Driver"),
    JDBC_NULL("DBUtilErrorCode-20", "JDBC URL为空，请检查配置"),
    CONF_ERROR("DBUtilErrorCode-00", "您的配置错误."),
    CONN_DB_ERROR("DBUtilErrorCode-10", "连接数据库失败. 请检查您的 账号、密码、数据库名称、IP、Port或者向 DBA 寻求帮助(注意网络环境)."),
    UNSUPPORTED_TYPE("DBUtilErrorCode-12", "不支持的数据库类型. 请注意查看 DataX 已经支持的数据库类型以及数据库版本."),
    COLUMN_SPLIT_ERROR("DBUtilErrorCode-13", "根据主键进行切分失败."),
    SET_SESSION_ERROR("DBUtilErrorCode-14", "设置 session 失败."),
    RS_ASYNC_ERROR("DBUtilErrorCode-15", "异步获取ResultSet next失败."),
    REQUIRED_VALUE("DBUtilErrorCode-03", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DBUtilErrorCode-02", "您填写的参数值不合法."),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "您填写的主键列不合法, DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型."),
    SPLIT_FAILED_ILLEGAL_SQL("DBUtilErrorCode-15", "DataX尝试切分表时, 执行数据库 Sql 失败. 请检查您的配置 table/splitPk/where 并作出修改."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "执行数据库 Sql 失败, 请检查您的配置的 querySql或者向 DBA 寻求帮助."),
    READ_RECORD_FAIL("DBUtilErrorCode-07", "读取数据库数据失败. 请检查您的配置的 querySql或者向 DBA 寻求帮助."),
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败."),
    NO_INSERT_PRIVILEGE("DBUtilErrorCode-11", "数据库没有写权限，请联系DBA"),
    NO_DELETE_PRIVILEGE("DBUtilErrorCode-16", "数据库没有DELETE权限，请联系DBA");

    private final String code;
    private final String description;

    private HANAReaderErrorCode(String code, String description) {
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
