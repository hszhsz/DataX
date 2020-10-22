package cn.ctyun.datax.plugin.reader.hanareader;
import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.DestinationDataProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 * @author devel
 */
public class SAPConn2 {
    private static final String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";

    static{
        Properties connectProperties = new Properties();
        //服务器
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "192.168.33.61");
        //系统编号
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,  "00");
        //SAP集团
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "810");
        //SAP用户名
        connectProperties.setProperty(DestinationDataProvider.JCO_USER,   "ykrfc_bd");
        //密码
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "ykrfcbd2020");
        //登录语言
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG,   "zh");
        //最大连接数
        connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "0");
        //最大连接线程
        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, "10");

        createDataFile(ABAP_AS_POOLED, "jcoDestination", connectProperties);
    }

    /**
     * 创建SAP接口属性文件。
     * @param name          ABAP管道名称
     * @param suffix        属性文件后缀
     * @param properties    属性文件内容
     */
    private static void createDataFile(String name, String suffix, Properties properties){
        File cfg = new File(name+"."+suffix);
        if(cfg.exists()){
            cfg.deleteOnExit();
        }
        try{
            FileOutputStream fos = new FileOutputStream(cfg, false);
            properties.store(fos, "for tests only !");
            fos.close();
        }catch (Exception e){
            System.out.println("Create Data file fault, error msg: " + e.toString());
            throw new RuntimeException("Unable to create the destination file " + cfg.getName(), e);
        }
    }

    /**
     * 获取SAP连接
     * @return  SAP连接对象
     */
    public static JCoDestination connect() throws Exception{
        JCoDestination destination =null;
        try {
            destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
            //JCoFunction是一个接口，代表SAP系统的函数库
            JCoRepository repository = destination.getRepository();
            // 从这个函数模板获得该SAP函数的对象
            JCoFunction function = repository.getFunction("RFC_READ_TABLE");
            //---------------第二步,获取VBRK信息 --------------------------
            JCoParameterList inParm =function.getImportParameterList();
            //设置参数
            inParm.setValue("QUERY_TABLE", "SKA1");
            inParm.setValue("DELIMITER", '\t');
//            inParm.setValue("NO_DATA", 'X');
//            inParm.setValue("ROWCOUNT",10);

//            JCoTable fieldsTable = function.getTableParameterList().getTable("FIELDS");
//            fieldsTable.appendRow();
//            fieldsTable.setValue("FIELDNAME", "OIHANTYP_GI"); //Sales Document
//            fieldsTable.appendRow();
//            fieldsTable.setValue("FIELDNAME", "OIO_VSTEL"); // Sales Document Item
//            fieldsTable.appendRow();
//            fieldsTable.setValue("FIELDNAME", "OIO_SPROC"); // Material number

            inParm = function.getTableParameterList();
//            JCoTable tableIn = inParm.getTable("DATA");//得到SAP函数中的表
//            JCoTable tableInn = inParm.getTable("FIELDS");//得到SAP函数中的表列名
//            tableInn.appendRow();//添加一行
//            tableInn.setValue("NAME1", "FIELDNAME");//
//            tableInn.appendRow();//添加一行
//            tableInn.setValue("NAME2", "FIELDNAME");//
            //增加条件
//            JCoTable tableInop = inParm.getTable("OPTIONS");//得到SAP函数中的条件参数
//            tableInop.appendRow();//添加一行
//            tableInop.setValue(" NAME1 like '%北京%' ",0);
//            function.setTableParameterList(inParm);
            function.execute(destination);
//            JCoTable ret = function.getTableParameterList().getTable("FIELDS");
//            JCoRecordFieldIterator fieldIterator = ret.getRecordFieldIterator();
//            while (fieldIterator.hasNextField()) {
//                    JCoRecordField jCoRecordField = fieldIterator.nextRecordField();
//                    jCoRecordField.getType();
//                    System.out.println(jCoRecordField.getName()+":" + jCoRecordField.getTypeAsString() + ":" + jCoRecordField.getType());
//            }

//            JCoTable ret = function.getTableParameterList().getTable("FIELDS");
            JCoTable ret = function.getTableParameterList().getTable("DATA");
            System.out.println("columns:" + ret.getNumColumns() + ",rows:" + ret.getNumRows());
            for(int i = 0; i < ret.getNumRows(); i++) {
                ret.setRow(i);
                System.out.println(i + ":" + ret.getValue(0));
//                System.out.println(i + ":" + ret.getValue("FIELDNAME") + ":" + ret.getValue("TYPE") + ":" +  ret.getValue("FIELDTEXT") + ":" + ret.getValue("LENGTH"));
            }
//            for (int i = 0; i < ret.getNumRows(); i++) {
//                ret.setRow(i);//指定行
//                ret.getMetaData().getName();
//                WA=new String(ret.getString("WA").getBytes("iso-8859-1"),"GB2312");
//                System.out.println(WA);
//            }
        } catch (JCoException e) {
            System.out.println("Connect SAP fault, error msg: " + e.toString());
        }
        return destination;
    }

    public static void workWithTable() throws JCoException {
        JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        JCoFunction function = destination.getRepository().getFunction("RFC_READ_TABLE");//从对象仓库中获取 RFM 函数：获取公司列表
        if (function == null)
            throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");
        try {
            function.execute(destination);
        } catch (AbapException e) {
            System.out.println(e.toString());
            return ;
        }

//        JCoStructure returnStructure = function.getExportParameterList().getStructure("return");

//        //判断读取是否成功
//        if (!(returnStructure.getString("TYPE").equals("") || returnStructure
//                .getString("TYPE").equals("S"))) {
//            throw new RuntimeException(returnStructure.getString("MESSAGE"));
//        }

        //获取Table参数：COMPANYCODE_LIST
        JCoTable codes = function.getTableParameterList().getTable("ACDOCA_LIST");
        for (int i = 0; i < codes.getNumRows(); i++) {//遍历Table
            codes.setRow(i);//将行指针指向特定的索引行
//            System.out.println(codes.getString("COMP_CODE") + '\t'
//                    + codes.getString("COMP_NAME"));
        }
//
//        // move the table cursor to first row
//        codes.firstRow();//从首行开始重新遍历 codes.nextRow()：如果有下一行，下移一行并返回True
//        for (int i = 0; i < codes.getNumRows(); i++, codes.nextRow()) {
//            //进一步获取公司详细信息
//            function = destination.getRepository().getFunction("BAPI_COMPANYCODE_GETDETAIL");
//            if (function == null)
//                throw new RuntimeException("BAPI_COMPANYCODE_GETDETAIL not found in SAP.");
//
//            function.getImportParameterList().setValue("COMPANYCODEID", codes.getString("COMP_CODE"));
//
//            function.getExportParameterList().setActive("COMPANYCODE_ADDRESS", false);
//
//            try {
//                function.execute(destination);
//            } catch (AbapException e) {
//                System.out.println(e.toString());
//                return ;
//            }
//
//            JCoStructure detail = function.getExportParameterList().getStructure("COMPANYCODE_DETAIL");
//
//            System.out.println(detail.getString("COMP_CODE") + '\t'
//                    + detail.getString("COUNTRY") + '\t'
//                    + detail.getString("CITY"));
//        }// for
    }

    public static void main(String[] args) throws Exception{
//        long startTime=System.currentTimeMillis();
//        for(int i = 0; i < 500; i++) {
//            connect();
//        }
//        long endTime=System.currentTimeMillis();
//        float excTime=(float)(endTime-startTime)/1000;
//        System.out.println("执行时间："+excTime+"s");
//        connect();
//        workWithTable();

        connect();
    }
}