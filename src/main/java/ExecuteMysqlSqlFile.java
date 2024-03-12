import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/7 9:54
 */
public class ExecuteMysqlSqlFile {

    public static void main(String[] args) throws Exception {

        // MySQL连接信息
        String url = "jdbc:mysql://192.168.172.103:3306/test";
        String user = "root";
        String password = "Y@cp3winer";

        String folderPath = "D:\\工作\\nanjing\\nj_clueflow";

        // 获取所有sql文件
        File folder = new File(folderPath);
        File[] sqlFiles = folder.listFiles(file -> file.getName().endsWith(".sql"));

        // 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);

        for (int i = 0; i < sqlFiles.length; i++) {

            System.out.println("执行文件"+sqlFiles[i].getName());
            Statement stmt = conn.createStatement();
            // 读取文件内容
            String sql = getFileContent(sqlFiles[i].getAbsolutePath());
            stmt.execute(sql);
            stmt.close();
        }

        // 关闭连接
        conn.close();

    }

    private static String getFileContent(String filePath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filePath)));
    }
}
