package jco;

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
            JCoFieldIterator it = inParm.getFieldIterator();
            while (it.hasNextField()) {
                System.out.println(it.nextField().getName());
            }
            //设置参数
            inParm.setValue("QUERY_TABLE", "ACDOCA");
            inParm.setValue("DELIMITER", '\t');
            inParm = function.getTableParameterList();
            JCoTable tableIn = inParm.getTable("DATA");//得到SAP函数中的表
            JCoTable tableInn = inParm.getTable("FIELDS");//得到SAP函数中的表列名
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
            String WA;
            JCoTable ret = function.getTableParameterList().getTable("DATA");
            for (int i = 0; i < ret.getNumRows(); i++) {
                ret.setRow(i);//指定行
                WA=new String(ret.getString("WA").getBytes("iso-8859-1"),"GB2312");
                System.out.println(WA);
            }
        } catch (JCoException e) {
            System.out.println("Connect SAP fault, error msg: " + e.toString());
        }
        return destination;
    }

    public static void main(String[] args) throws Exception{
//        long startTime=System.currentTimeMillis();
//        for(int i = 0; i < 500; i++) {
//            connect();
//        }
//        long endTime=System.currentTimeMillis();
//        float excTime=(float)(endTime-startTime)/1000;
//        System.out.println("执行时间："+excTime+"s");
        connect();
    }
}