package cn.ctyun.datax.plugin.reader.hanareader;

/**
 * HANA常量类
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public class KeyConstant {

    /**
     * HANA driver path
     */
    public static final String DRIVER = "com.sap.db.jdbc.Driver";
    /**
     * JDBC Url
     */
    public static final String JDBC_URL = "jdbcUrl";
    /**
     * HANAdb 的用户名
     */
    public static final String HANA_USERNAME = "username";
    /**
     * HANAdb 密码
     */
    public static final String HANA_PASSWORD = "password";

    public static final String HANA_HOST = "host";

    public static final String HANA_PORT = "port";


    public static final String FETCH_SIZE = "fetchSize";

    public static final String QUERY_SQL = "querySql";

    public static final String TABLE = "table";

    public static final String CONN_MARK = "connection";

    public static final String MANDATORY_ENCODING = "mandatoryEncoding";

    /**
     * HANAdb 查询条件
     */
    public static final String HANA_QUERY = "query";
    /**
     * HANAdb 的列
     */
    public static final String HANA_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 跳过的列数
     */
    public static final String SKIP_COUNT = "skipCount";


    public static final String LOWER_BOUND = "lowerBound";
    public static final String UPPER_BOUND = "upperBound";
    public static final String IS_OBJECTID = "isObjectId";
    /**
     * 批量获取的记录数
     */
    public static final String BATCH_SIZE = "batchSize";
    /**
     * HANADB的错误码
     */
    public static final int HANA_UNAUTHORIZED_ERR_CODE = 13;
    public static final int HANA_ILLEGALOP_ERR_CODE = 20;
}
