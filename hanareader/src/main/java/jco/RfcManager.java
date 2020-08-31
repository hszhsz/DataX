package jco;

import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.Environment;
import java.util.Properties;

public final class RfcManager {
    private static final String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    private static JCOProvider provider;
    private static JCoDestination destination;
    static {
        Properties properties = loadProperties();
        // catch IllegalStateException if an instance is already registered
        try {
            provider = new JCOProvider();
            Environment.registerDestinationDataProvider(provider);
            provider.changePropertiesForABAP_AS(ABAP_AS_POOLED, properties);
        } catch (IllegalStateException e) {
            System.out.println(e.getMessage());
        }
    }

    public static Properties loadProperties() {
        Properties props=new Properties();
        props.setProperty("jco.client.user","yk-lixu");
        props.setProperty("jco.client.passwd","12345678");
        props.setProperty("jco.client.lang", "en");
        props.setProperty("jco.client.client","000");
        props.setProperty("jco.client.sysnr","00");
        props.setProperty("jco.client.ashost","192.168.33.21");
        props.setProperty("jco.destination.peak_limit","10");
        props.setProperty("jco.destination.pool_capacity","3");
        return props;
    }

    public static JCoDestination getDestination() throws JCoException {
        if (destination == null) {
            destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
        }
        return destination;
    }

    public static void execute(JCoFunction function) {
        System.out.println("SAP Function Name : " + function.getName());
        try {
            function.execute(getDestination());
        } catch (JCoException e) {
            e.printStackTrace();
        }
    }

    public static JCoFunction getFunction(String functionName) {
        JCoFunction function = null;
        try {
            function = getDestination().getRepository().getFunctionTemplate(functionName).getFunction();
        } catch (JCoException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
        return function;
    }
}