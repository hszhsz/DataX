//package jco;
//
//import com.sap.conn.jco.JCoFunction;
//
//public class JcoSample {
//    private static JCO.Client theConnection;
//    private static IRepository theRepository;
//    private static final String POOL_NAME = "myPool";
//
//    public static void main(String[] args) {
//
//        JcoPool connPool = JCO.getClientPoolManager().getPool(POOL_NAME);
//        if (connPool == null) {
//            JCO.addClientPool(POOL_NAME,
//                    5,      //number of connections in the pool
//                    "client",
//                    "username",
//                    "paswword",
//                    "EN",
//                    "hostname",
//                    "00");
//        }
//
//        theConnection = JCO.getClient(POOL_NAME);
//        retrieveRepository();
//        try {
//            JCO.Function function = getFunction("RFC_READ_TABLE");
//            JCO.ParameterList listParams = function.getImportParameterList();
//
//            listParams.setValue("BSAUTHORS", "QUERY_TABLE");
//
//            theConnection.execute(function);
//
//            JCO.Table tableList = function.getTableParameterList().getTable("DATA");
//
//            if (tableList.getNumRows() > 0) {
//                do {
//                    for (JCO.FieldIterator fI = tableList.fields();
//                         fI.hasMoreElements();)
//                    {
//                        JCO.Field tabField = fI.nextField();
//                        System.out.println(tabField.getName()
//                                + ":t" +
//                                tabField.getString());
//                    }
//                    System.out.println("n");
//                }
//                while (tableList.nextRow() == true);
//            }
//        }
//        catch (Exception ex) {
//            ex.printStackTrace();
//        }
//
//        JCO.releaseClient(theConnection);
//
//    }
//
//    private static void retrieveRepository() {
//        try {
//            theRepository = new JCO.Repository("saprep", theConnection);
//        }
//        catch (Exception ex)
//        {
//            System.out.println("failed to retrieve repository");
//        }
//    }
//    public static JCoFunction getFunction(String name) {
//        try {
//            return theRepository.getFunctionTemplate(name.toUpperCase()).getFunction();
//        }
//        catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        return null;
//    }
//}