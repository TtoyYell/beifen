import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/5 10:06
 */
public class FindExtraIDs {

    public static void main(String[] args) throws IOException {
        Set<String> sqlIds = readIdsFromSql();
        Set<String> csvIds = readIdsFromCsv();

        Set<String> extraIds = new HashSet<>(sqlIds);
        extraIds.removeAll(csvIds);

        Set<String> sqls = findSqlByIds(extraIds);

        sqls.forEach(System.out::println);

    }

    private static Set<String> findSqlByIds(Set<String> extraIds) throws IOException {
        Set<String> sqls = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Admin\\Desktop\\测试文件\\数据库导入\\xianxia_suspect_illegal.sql"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if(line.startsWith("INSERT")) {
                    // 获取insert语句的值部分
                    String values = line.split("VALUES")[1];
                    // 从values语句中获取id字段
                    String id = values.split(",")[1];
                    id = id.trim().replaceAll("'","");
                    if (extraIds.contains(id)){
                        sqls.add(line);
                    }
                }
            }
        }
        return sqls;
    }

    private static Set<String> readIdsFromCsv() throws IOException {
        Set<String> ids = new HashSet<>();
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Admin\\Desktop\\测试文件\\数据库导入\\xianxia_suspect_illegal.csv"));

        // 读取第一行为字段名
        br.readLine();

        String line;
        while ((line = br.readLine()) != null) {
            String[] cols = line.split(",");
            ids.add(cols[1]);
        }

        br.close();
        return ids;
    }

    private static Set<String> readIdsFromSql() throws IOException {
        Set<String> ids = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Admin\\Desktop\\测试文件\\数据库导入\\xianxia_suspect_illegal.sql"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if(line.startsWith("INSERT")) {
                    // 获取insert语句的值部分
                    String values = line.split("VALUES")[1];
                    // 从values语句中获取id字段
                    String id = values.split(",")[1];
                    id = id.trim().replaceAll("'","");
                    ids.add(id);
                }
            }
        }
        return ids;
    }

}
