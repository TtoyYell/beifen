package beian;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/11 15:19
 */
public class HuwaiBeianData {

    public static void main(String[] args) throws Exception {

        String targetFile = "C:\\Users\\Admin\\Desktop\\数据.sql";
        FileWriter writer = new FileWriter(targetFile);

        Connection postgreConnection = getPostgreConnection();

        Connection mysqlConnection = getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();
        String sql = " select  user_id, " +
                "company_name as name,  " +
                "'企业用户' as type,  " +
                "user_name as username,  " +
                "address as addr,  " +
                "type_name as enterprise,  " +
                "corporate as legal,  " +
                "license_id as license,  " +
                "real_name as user,  " +
                "user_mobile as mobile,  " +
                " DATE_FORMAT(FROM_UNIXTIME(create_time), '%Y-%m-%d') AS create_time " +
                "from user a left join company_type b  " +
                "on a.company_type = b.type_id ";
        ResultSet resultSet = stmt.executeQuery(sql);
        List<Map<String, Object>> mapList = resultSetToListMap(resultSet);
        for (int i = 2547; i < mapList.size()+2547; i++) {
            Statement insertStmt = postgreConnection.createStatement();
            String insertSql = " INSERT INTO \"customer\" ( " +
                    " \"id\",  " +
                    " \"username\",  " +
                    " \"password\",  " +
                    " \"serial\",  " +
                    " \"mobile\",  " +
                    " \"name\",  " +
                    " \"identification\",  " +
                    " \"department_id\",  " +
                    " \"character\",  " +
                    " \"role\",  " +
                    " \"modify_password\",  " +
                    " \"disable_login\",  " +
                    " \"user_type\"   " +
                    ")  " +
                    "VALUES  " +
                    " (  " +
                    "  "+i+",  " +
                    "  '"+mapList.get(i-2547).get("username")+"',  " +
                    "  '$2b$12$zv.hzoWu5600MVq2Rup0pO1YQWGrLKukD800/jGQmYCeqsqsDr8OS',  " +
                    "  '001',  " +
                    "  '"+mapList.get(i-2547).get("mobile")+"',  " +
                    "  '"+mapList.get(i-2547).get("name")+"',  " +
                    "  NULL,  " +
                    "  401,  " +
                    "  8,  " +
                    "  0,  " +
                    "  't',  " +
                    "  '2023-12-09 00:07:22.254425+08',  " +
                    "  2   " +
                    " );";
            System.out.println(insertSql);
            writer.write(insertSql+"\n");
            writer.flush();

            String insertSql2 = " INSERT INTO \"public\".\"enterprise_users\"(\"id\" ,\"created_time\", \"updated_time\", \"updated_by\", " +
                    "\"name\", \"type\", \"username\", \"addr\", \"validity\", \"enterprise\", \"legal\", \"license\", \"user\", \"mobile\", " +
                    "\"customer_id\") VALUES ("+mapList.get(i-2547).get("user_id")+","+forDateNull(mapList.get(i-2547).get("create_time"))+", NULL, NULL, '"+mapList.get(i-2547).get("name")+"', " +
                    "'企业用户', '"+mapList.get(i-2547).get("username")+"', '"+mapList.get(i-2547).get("addr")+"', 'f', " +
                    "'"+mapList.get(i-2547).get("enterprise")+"', '"+mapList.get(i-2547).get("legal")+"', '"+mapList.get(i-2547).get("license")+"'," +
                    " '"+mapList.get(i-2547).get("user")+"', '"+mapList.get(i-2547).get("mobile")+"', "+i+");";
            System.out.println(insertSql2);
            writer.write(insertSql2+"\n");
            writer.flush();

            if (false) {
                insertStmt.execute(insertSql);
                insertStmt.execute(insertSql2);
            }

        }

        stmt.close();
        // 关闭连接
        mysqlConnection.close();

    }

    private static String forDateNull(Object value) {
        if (value==null || "".equals(value)) {
            return "NULL";
        } else {
            return "'"+Str(value)+"'";
        }
    }

    public static List<Map<String, Object>> resultSetToListMap(ResultSet resultSet) throws SQLException {

        List<Map<String, Object>> list = new ArrayList<>();

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        while(resultSet.next()) {

            Map<String, Object> row = new HashMap<>();

            for(int i=1; i<=columnCount; i++) {

                String columnName = metaData.getColumnName(i);
                String columnLabel = metaData.getColumnLabel(i);

                String key = columnLabel;
                if(columnLabel == null || columnLabel.equals(columnName)) {
                    key = columnName;
                }

                row.put(key, resultSet.getObject(i));

            }

            list.add(row);

        }
        return list;

    }

    private static String Str(Object str) {
        return String.valueOf(str);
    }

    public static Connection getPostgreConnection() throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://192.168.172.103:5432/postgres";
        String user = "postgres";
        String password = "y@cp3winer";
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    private static Connection getMysqlConnection() throws SQLException {
        // MySQL连接信息
        String url = "jdbc:mysql://192.168.172.103:3306/huwaibeian";
        String user = "root";
        String password = "Y@cp3winer";

        // 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

}
