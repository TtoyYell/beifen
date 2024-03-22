import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/7 9:26
 */
public class SqlFileMerger {
    public static void main(String[] args) throws IOException {

        String folderPath = "D:\\工作\\njhw\\user";

        // 获取所有sql文件
        File folder = new File(folderPath);
        File[] sqlFiles = folder.listFiles(file -> file.getName().endsWith(".sql"));

        // 合并文件路径
        String mergedFilePath = "D:\\工作\\all.sql";

        // 合并文件写出
        BufferedWriter bw = new BufferedWriter(new FileWriter(mergedFilePath));

        for (int i = 0; i < sqlFiles.length; i++) {
            // 读取文件内容
            List<String> lines = Files.readAllLines(Paths.get(sqlFiles[i].getPath()));

            for (String line : lines) {
                // 写入内容
                bw.write(line);
                bw.write("\n");
            }

            if (sqlFiles.length-1 != i) {
                // 每个文件之间添加分隔符
                bw.write(";");
            }
        }


        bw.close();

    }
}
