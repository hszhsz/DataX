package jco;

import java.io.File;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.DestinationDataProvider;

/**
 * @author devel
 */
public class SAPConn {
    private static final String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    static{
        Properties connectProperties = new Properties();
        //服务器
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "192.168.33.21");
        //系统编号
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,  "00");
        //SAP集团
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "100");
        //SAP用户名
        connectProperties.setProperty(DestinationDataProvider.JCO_USER,   "yk-lixu");
        //密码
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "12345678");
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
    public static JCoDestination connect(){
        JCoDestination destination =null;
        try {
            destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
            //JCoFunction是一个接口，代表SAP系统的函数库
            JCoRepository repository = destination.getRepository();
            // 从这个函数模板获得该SAP函数的对象
            JCoFunction function = repository.getFunction("BAPI_COMPANYCODE_GETLIST");

            // 获得函数的import参数列表
//            JCoParameterList input = function.getImportParameterList();
            function.execute(destination);
            // 获得Export变量列表。
//            JCoParameterList output = function.getExportParameterList();

            //获取表
            JCoParameterList tables = function.getTableParameterList();

//            System.out.println(tables.getTable(0));

            JCoTable bt = tables.getTable("COMPANYCODE_LIST");

//            for (int i = 0; i < bt.getNumRows(); i++) {
//                bt.setRow(i);
//
//                System.out.println("COMP:" + bt.getString("COMP_CODE"));
//                System.out.println("COMP NAME:" + bt.getString("COMP_NAME"));
//            }
        } catch (JCoException e) {
            System.out.println("Connect SAP fault, error msg: " + e.toString());
        }
        return destination;
    }

    public static void main(String[] args) {
        long startTime=System.currentTimeMillis();
        for(int i = 0; i < 500; i++) {
            connect();
        }
        long endTime=System.currentTimeMillis();
        float excTime=(float)(endTime-startTime)/1000;
        System.out.println("执行时间："+excTime+"s");
    }
}