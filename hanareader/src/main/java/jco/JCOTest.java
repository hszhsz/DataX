package jco;

import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoTable;

import java.util.ArrayList;
import java.util.List;

public class JCOTest {

    public static void main(String[] args)
    {
        getUser();
    }
    public static List<User> getUser() {

        JCoFunction function = RfcManager.getFunction("BAPI_COMPANYCODE_GETLIST");

        RfcManager.execute(function);
        JCoParameterList outputParam = function.getTableParameterList();
        JCoTable bt = outputParam.getTable("TABLEOUT");
        List<User> list = new ArrayList<User>();
        for (int i = 0; i < bt.getNumRows(); i++) {
            bt.setRow(i);

            User user = new User();
            user.setUserName(bt.getString("USER_NAME"));
            list.add(user);

            System.out.println(bt.getString("USER_NAME"));
        }
        return list;
    }
}