import java.sql.*;
import java.io.*;
import java.lang.*;

class SalaryStdDev{
    public static void main(String args[]) throws SQLException {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        System.out.println("driver loaded");
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs =null;
        String user = args[1];
        String pass = args[2];
        String db = args[0];
        float sum = 0;
        float square_sum = 0;
        float sal = 0;
        int count = 0;
        try{
            conn = DriverManager.getConnection("jdbc:db2://localhost:50000/" + db, user, pass);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        statement = conn.prepareStatement("SELECT salary FROM employee" );
        rs = statement.executeQuery();
        while(rs.next()){
            sal = Float.parseFloat(rs.getString("salary"));
            sum += sal;
            square_sum += sal*sal;
            count +=1;
        }
        double mean = sum/count;
        double temp = (square_sum/count) - (mean*mean);
        double std = Math.sqrt(temp);
        System.out.println("Standard Deviation of Salary: " + std);
    }
}