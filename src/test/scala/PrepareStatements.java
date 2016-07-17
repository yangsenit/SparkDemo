import com.mysql.jdbc.Statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by ys on 16-7-17.
 */
public class PrepareStatements {
    public static void main(String [] args)throws Exception{
        Class.forName("com.mysql.jdbc.Driver");
        String url="jdbc:mysql://localhost:3306/ys";
        String user="root";
        String password="";
        Connection conn = DriverManager.getConnection(url,user,password);
        if(conn==null){
            System.out.print("sajdflka");
        }
        //String sql ="insert into AccumulatorAndBroadcastDemo （notblackname） values （?）";
        String sql="insert into AccumulatorAndBroadcastDemo (notblackname) values (?)";
        PreparedStatement state = conn.prepareStatement(sql);
        state.setString(1,"hello1");
        //Statement state= (Statement) conn.createStatement();
        state.executeUpdate();
    }
}
