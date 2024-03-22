package beian;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/11 15:19
 */
public class HuwaiBeianData2 {


    private static Map<String, String> mimeTypeMap = new HashMap<>();
    static {
        // 图片类型
        mimeTypeMap.put("jpg", "image/jpeg");
        mimeTypeMap.put("jpeg", "image/jpeg");
        mimeTypeMap.put("png", "image/png");
        mimeTypeMap.put("gif", "image/gif");
        mimeTypeMap.put("bmp", "image/bmp");

        // PDF
        mimeTypeMap.put("pdf", "application/pdf");

        // 视频类型
        mimeTypeMap.put("mp4", "video/mp4");
        mimeTypeMap.put("mov", "video/quicktime");
        mimeTypeMap.put("avi", "video/x-msvideo");
        mimeTypeMap.put("mkv", "video/x-matroska");
        // 音频类型
        mimeTypeMap.put("mp3", "audio/mpeg");
        mimeTypeMap.put("wav", "audio/wav");
        mimeTypeMap.put("ogg", "audio/ogg");
        mimeTypeMap.put("flac", "audio/flac");
        mimeTypeMap.put("aac", "audio/aac");
        mimeTypeMap.put("m4a", "audio/m4a");
    }

    public static void main(String[] args) throws Exception {

        String targetFile = "C:\\Users\\Admin\\Desktop\\数据2.sql";
        FileWriter writer = new FileWriter(targetFile);

        Connection postgreConnection = getPostgreConnection();

        Connection mysqlConnection = getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();
        String sql = " SELECT  " +
                "ad_id,user_id,addemo,adtable,adlicense,adaddress,adpublic,adcontent,ademployer,adother,adaudio,video_key,ad_log,record_name,audit_opinion,audit_time,apply_id, " +
                "ad_name, ad_type_type,ad_pub_sdate as stime,ad_pub_edate as etime," +
                "ad_sponsor_address,ad_sponsor_company_name,ad_sponsor_company_type,ad_sponsor_corporate,ad_sponsor_license_id,ad_sponsor_user_mobile," +
                "DATE_FORMAT(FROM_UNIXTIME(create_time), '%Y-%m-%d %H:%i:%s') AS create_time, " +
                "b.aat_name as area_type, " +
                "c.as_name as modus, " +
                "d.at_name as type, " +
                "ad_pub_duration as duration, " +
                "ad_pub_size as acreage, " +
                "ad_pub_pixel as pixel, " +
                "ad_address as area, " +
                "NULL as deadline, " +
                "NULL as  banner, " +
                "ad_time_interval, " +
                "plane_ad_pub_num as number, " +
                "video_ad_pub_num as video, " +
                "ad_pub_price as price, " +
                "case when ad_status = 10 then '未查阅' when ad_status = 11 then '已查阅' when ad_status = 12 then '退回'  else '未提交' end as view " +
                "FROM `ad_data` as a   " +
                "left join ad_address_type as b on a.ad_address_type = b.aat_id " +
                "left join ad_style as c on a.ad_style = c.as_id " +
                "left join ad_type as d on a.ad_type = d.at_id ";
        ResultSet resultSet = stmt.executeQuery(sql);
        List<Map<String, Object>> mapList = resultSetToListMap(resultSet);
        for (int i = 0; i < mapList.size(); i++) {
            String cpmpanyId = Str(mapList.get(i).get("user_id"));

            String companyName = "";
            String addr = "";
            String enterprise = "";
            String license = "";
            String user = "";
            String mobile = "";

            companyName = Str(mapList.get(i).get("ad_sponsor_company_name"));
            addr = Str(mapList.get(i).get("ad_sponsor_address"));
            enterprise = getCompanyType(Str(mapList.get(i).get("ad_sponsor_company_type")));
            license = Str(mapList.get(i).get("ad_sponsor_license_id"));
            user = Str(mapList.get(i).get("ad_sponsor_corporate"));
            mobile = Str(mapList.get(i).get("ad_sponsor_user_mobile"));

            Statement insertStmt = postgreConnection.createStatement();

            String insertTaskSql = " INSERT INTO \"public\".\"pub_task\"(" +
                    "\"id\", \"created_time\", \"updated_time\", \"updated_by\", \"name\", " +
                    "\"start\", \"end\", \"submit\", \"note\", \"type\", \"is_delete\") VALUES (" +
                    ""+(i+1)+", '"+mapList.get(i).get("create_time")+"', NULL, NULL, '"+NullToBlank(mapList.get(i).get("ad_name"))+"', '"+mapList.get(i).get("create_time")+"', " +
                    "'"+mapList.get(i).get("create_time")+"', '"+mapList.get(i).get("create_time")+"', '', "+mapList.get(i).get("ad_type_type")+", 'f'); ";
            System.out.println(insertTaskSql);
            writer.write(insertTaskSql+"\n");
            writer.flush();

            String insertPartSql = " INSERT INTO \"public\".\"pub_part\"(" +
                    "\"created_time\",  \"name\", \"addr\", \"enterprise\", \"license\", \"number\", \"user\", \"mobile\", " +
                    "\"price\", \"scale\",\"save\", \"check\", \"view\", " +
                    "\"note\", \"view_at\", \"task\", \"users\", \"submit_at\", " +
                    "\"submit_num\") VALUES (" +
                    "'"+mapList.get(i).get("create_time")+"', '"+companyName+"', '"+addr+"', '"+enterprise+"', '"+license+"', '"+ NullToBlank(mapList.get(i).get("record_name"))+"', '"+user+"', '"+mobile+"', " +
                    "'"+NullToBlank(mapList.get(i).get("ad_pub_price"))+"', '', 't', "+getViewLog(mapList.get(i).get("ad_log"))+", "+getAdvView(mapList.get(i).get("view"))+", " +
                    "'"+NullToBlank(mapList.get(i).get("audit_opinion"))+"', "+getViewAt(mapList.get(i).get("audit_time"))+", "+(i+1)+", "+cpmpanyId+", '"+mapList.get(i).get("create_time")+"', " +
                    "'"+mapList.get(i).get("apply_id")+"'); ";
            System.out.println(insertPartSql);
            writer.write(insertPartSql+"\n");
            writer.flush();

            String addemo = Str(mapList.get(i).get("addemo"));
            String adtable = Str(mapList.get(i).get("adtable"));
            String adlicense = Str(mapList.get(i).get("adlicense"));
            String adaddress = Str(mapList.get(i).get("adaddress"));
            String adpublic = Str(mapList.get(i).get("adpublic"));
            String adcontent = Str(mapList.get(i).get("adcontent"));
            String ademployer = Str(mapList.get(i).get("ademployer"));
            String adother = Str(mapList.get(i).get("adother"));
            String adaudio = Str(mapList.get(i).get("adaudio"));
            String video_key = Str(mapList.get(i).get("video_key"));
            String fileJson = getFileJson(addemo,adtable,adlicense,adaddress,adpublic,adcontent,ademployer,adother,adaudio,video_key);

            String insertSql = " INSERT INTO \"public\".\"pub_adv\" ( " +
                    " \"created_time\", " +
                    " \"name\", " +
                    " \"type\", " +
                    " \"modus\", " +
                    " \"play_period\", " +
                    " \"duration\", " +
                    " \"acreage\", " +
                    " \"pixel\", " +
                    " \"archives\", " +
                    " \"area\", " +
                    " \"deadline\", " +
                    " \"banner\", " +
                    " \"number\", " +
                    " \"video\", " +
                    " \"price\", " +
                    " \"area_type\", " +
                    " \"view\", " +
                    " \"task\", " +
                    " \"users\", " +
                    " \"filing\" " +
                    ") " +
                    "VALUES " +
                    " ( " +
                    " '"+mapList.get(i).get("create_time")+"', " +
                    " '"+NullToBlank(mapList.get(i).get("ad_name"))+"', " +
                    " "+getType(mapList.get(i).get("type"))+", " +
                    " "+getStyle(mapList.get(i).get("modus"))+", " +
                    " "+getPlayTime(Str(mapList.get(i).get("ad_time_interval")))+", " +
                    " "+mapList.get(i).get("duration")+", " +
                    " "+mapList.get(i).get("acreage")+", " +
                    " "+mapList.get(i).get("pixel")+", " +
                    " '"+fileJson+"', " +
                    " '"+NullToBlank(mapList.get(i).get("area"))+"', " +
                    " "+getDeadLine(mapList.get(i).get("stime"),mapList.get(i).get("etime"))+", " +
                    " NULL, " +
                    " "+mapList.get(i).get("number")+", " +
                    " "+mapList.get(i).get("video")+", " +
                    " "+mapList.get(i).get("price")+", " +
                    " "+getAreaType(mapList.get(i).get("area_type"))+", " +
                    " "+getAdvView(mapList.get(i).get("view"))+", " +
                    " "+(i+1)+", " +
                    " "+mapList.get(i).get("user_id")+", " +
                    " NULL " +
                    " ); ";
            System.out.println(insertSql);
            writer.write(insertSql+"\n");
            writer.flush();


            if (false) {
                insertStmt.execute(insertSql);
                insertStmt.execute(insertTaskSql);
                insertStmt.execute(insertPartSql);
            }

        }

        stmt.close();
        // 关闭连接
        mysqlConnection.close();

    }

    private static String getCompanyType(String ad_sponsor_company_type) {
        if ("1".equals(ad_sponsor_company_type)) {
            return "国有企业";
        } else if ("2".equals(ad_sponsor_company_type)) {
            return "国有事业";
        } else if ("3".equals(ad_sponsor_company_type)) {
            return "集体企业";
        } else if ("4".equals(ad_sponsor_company_type)) {
            return "集体事业";
        } else if ("5".equals(ad_sponsor_company_type)) {
            return "私营企业";
        } else if ("6".equals(ad_sponsor_company_type)) {
            return "个体工商户";
        } else if ("7".equals(ad_sponsor_company_type)) {
            return "联营企业";
        } else if ("8".equals(ad_sponsor_company_type)) {
            return "外商投资企业";
        } else if ("9".equals(ad_sponsor_company_type)) {
            return "事业单位";
        } else if ("10".equals(ad_sponsor_company_type)) {
            return "社会团体";
        } else if ("21".equals(ad_sponsor_company_type)) {
            return "个人";
        } else {
            return "其它";
        }
    }

    private static String getDeadLine(Object stime, Object etime) {
        if (stime==null&&etime==null) {
            return "NULL";
        } else if (stime!=null && etime == null) {
            return Str(stime);
        } else if (stime!=null && etime !=null) {
            return "'"+stime+"--"+etime+"'";
        } else {
            return "NULL";
        }
    }

    private static String getFileJson(String addemo, String adtable, String adlicense, String adaddress, String adpublic, String adcontent, String ademployer, String adother, String adaudio, String video_key) throws JsonProcessingException {
        List<Map<String,String>> resList = new ArrayList<>();

        setList(resList,addemo,"广告样件");
        setList(resList,adtable,"户外广告登记申请表");
        setList(resList,adlicense,"营业执照");
        setList(resList,adaddress,"场地或者设施的使用权证明");
        setList(resList,adpublic,"相关政府部门批准文件");
        setList(resList,adcontent,"户外广告内容证明文件");
        setList(resList,ademployer,"委托方材料");
        setList(resList,adother,"其他文件");
        setList(resList,adaudio,"音频文件");
        //video_key; 视频文件  http://njhw.xshz.vip/
        if (!"null".equals(video_key) && !"".equals(video_key)) {
            String[] infs = video_key.split(",");
            for (int i = 0; i < infs.length; i++) {
                String uuid = UUID.randomUUID().toString();
                uuid = "__AUTO__"+uuid.substring(uuid.lastIndexOf("-") + 1);
                String extension = infs[i].substring(infs[i].lastIndexOf(".") + 1);
                HashMap<String, String> map = new HashMap<>();
                map.put("url","http://njhw.xshz.vip/"+infs[i]);
                map.put("name","视频文件"+(i+1));
                map.put("type",mimeTypeMap.getOrDefault(extension, "application/octet-stream"));
                map.put("uid",uuid);
                resList.add(map);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        String res = mapper.writeValueAsString(resList);
        return res;
    }



    private static void setList(List<Map<String, String>> resList, String inf,String name) {
        if ("null".equals(inf)||"".equals(inf)) {
            return;
        }
        String[] infs = inf.split(",");
        for (int i = 0; i < infs.length; i++) {
            String uuid = UUID.randomUUID().toString();
            uuid = "__AUTO__"+uuid.substring(uuid.lastIndexOf("-") + 1);
            String extension = infs[i].substring(infs[i].lastIndexOf(".") + 1);
            HashMap<String, String> map = new HashMap<>();
            map.put("url",infs[i]);
            map.put("name",name+(i+1));
            map.put("type",mimeTypeMap.getOrDefault(extension, "application/octet-stream"));
            map.put("uid", uuid);
            resList.add(map);
        }
    }


    public static String getViewLog(Object adLog) throws JsonProcessingException {
        if (adLog == null || "".equals(adLog)) {
            return "NULL";
        }
        List<Map<String,String>> resList = new ArrayList<>();
        String log = Str(adLog);
        String[] logs = log.split("</br>");
        for (String s : logs) {
            if (s.contains("创建广告,存入草稿")) {
                continue;
            }
            if (s.contains("修改广告并提交平台查阅")||s.contains("创建广告,提交资料")) {
                HashMap<String, String> map = new HashMap<>();
                map.put("type","已提交");
                map.put("submit_at",s.substring(0,19));
                resList.add(map);
            }
            if (s.contains("工商查阅被退回")) {
                HashMap<String, String> map = new HashMap<>();
                map.put("type","建议修改");
                map.put("submit_at",s.substring(0,19));
                s = s.substring(s.indexOf("查阅人:"));
                // 正则表达式匹配
                Pattern pattern = Pattern.compile("查阅人:([^ ]+) +退回原因:([^\\.]+)");
                Matcher matcher = pattern.matcher(s);
                if(matcher.find()) {
                    String auditor = matcher.group(1);
                    String reason = matcher.group(2);
                    map.put("users",auditor);
                    map.put("note",reason);
                }
                resList.add(map);
            }
            if (s.contains("工商查阅通过")) {
                HashMap<String, String> map = new HashMap<>();
                map.put("type","已查阅");
                map.put("submit_at",s.substring(0,19));
                resList.add(map);
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        String string = mapper.writeValueAsString(resList);
        return "'"+string+"'";
    }


    private static String getViewAt(Object auditTime) {
        if (auditTime == null || "".equals(auditTime)) {
            return "NULL";
        }
        String time = Str(auditTime);
        long timestamp = Long.parseLong(time);
        timestamp *= 1000;
        Date date = new Date(timestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = sdf.format(date);
        return "'"+dateString+"'";
    }

    @Deprecated
    private static String getSubMitNum(Object create_time) {
        return null;// TODO
    }

    private static String getAdvView(Object view) {
        if ("未查阅".equals(Str(view))) {
            return "1";
        } else if("已查阅".equals(Str(view))) {
            return "2";
        } else if ("退回".equals(Str(view))) {
            return "3";
        }  else if ("未提交".equals(Str(view))) {
            return "0";
        } else {
            return "NULL";
        }
    }

    private static String getAreaType(Object areaType) {
        if ("住宅小区".equals(Str(areaType))) {
            return "1";
        } else if("楼宇内".equals(Str(areaType))) {
            return "2";
        } else if ("公共交通工具内".equals(Str(areaType))) {
            return "3";
        }  else if ("体育场馆内".equals(Str(areaType))) {
            return "4";
        } else if ("农贸市场".equals(Str(areaType))) {
            return "5";
        } else if ("专业市场".equals(Str(areaType))) {
            return "6";
        } else if ("影剧院内".equals(Str(areaType))) {
            return "7";
        } else if ("公交".equals(Str(areaType))) {
            return "8";
        } else if ("地铁".equals(Str(areaType))) {
            return "9";
        } else if ("主干道".equals(Str(areaType))) {
            return "10";
        } else if ("机场".equals(Str(areaType))) {
            return "11";
        } else if ("街巷".equals(Str(areaType))) {
            return "12";
        } else if ("火车站".equals(Str(areaType))) {
            return "13";
        } else if ("高速公路".equals(Str(areaType))) {
            return "14";
        } else if ("其它".equals(Str(areaType))) {
            return "15";
        } else {
            return "NULL";
        }
    }

    private static String NullToBlank(Object area) {
        if (area == null) {
            return "";
        } else return Str(area);
    }


    private static String getStyle(Object modus) {
        if ("广播".equals(Str(modus))) {
            return "2";
        } else if("电视".equals(Str(modus))) {
            return "1";
        } else if ("报纸".equals(Str(modus))) {
            return "3";
        }  else if ("期刊".equals(Str(modus))) {
            return "17";
        } else if ("互联网".equals(Str(modus))) {
            return "4";
        } else if ("固定印刷品".equals(Str(modus))) {
            return "17";
        } else if ("投影式".equals(Str(modus))) {
            return "17";
        } else if ("电子显示屏".equals(Str(modus))) {
            return "8";
        } else if ("展示牌".equals(Str(modus))) {
            return "7";
        } else if ("灯箱".equals(Str(modus))) {
            return "9";
        } else if ("霓虹灯".equals(Str(modus))) {
            return "10";
        } else if ("交通工具".equals(Str(modus))) {
            return "11";
        } else if ("水上漂浮物".equals(Str(modus))) {
            return "12";
        } else if ("升空器具".equals(Str(modus))) {
            return "13";
        } else if ("充气物".equals(Str(modus))) {
            return "14";
        } else if ("模型".equals(Str(modus))) {
            return "15";
        } else if ("户外路牌广告".equals(Str(modus))) {
            return "16";
        } else if ("其它".equals(Str(modus))) {
            return "17";
        }  else {
            return "NULL";
        }
    }

    private static String getType(Object type) {
        if ("政治政策类".equals(Str(type))) {
            return "1";
        } else if("社会文明类".equals(Str(type))) {
            return "2";
        } else if ("节日类".equals(Str(type))) {
            return "3";
        }  else if ("节日类".equals(Str(type))) {
            return "4";
        } else if ("节日类".equals(Str(type))) {
            return "5";
        } else if ("节日类".equals(Str(type))) {
            return "6";
        } else if ("节日类".equals(Str(type))) {
            return "7";
        } else if ("节日类".equals(Str(type))) {
            return "8";
        } else if ("节日类".equals(Str(type))) {
            return "9";
        } else if ("节日类".equals(Str(type))) {
            return "10";
        } else if ("节日类".equals(Str(type))) {
            return "11";
        } else if ("节日类".equals(Str(type))) {
            return "12";
        } else if ("节日类".equals(Str(type))) {
            return "13";
        } else if ("节日类".equals(Str(type))) {
            return "14";
        } else if ("节日类".equals(Str(type))) {
            return "15";
        } else if ("节日类".equals(Str(type))) {
            return "16";
        } else if ("节日类".equals(Str(type))) {
            return "17";
        } else if ("节日类".equals(Str(type))) {
            return "18";
        } else if ("节日类".equals(Str(type))) {
            return "19";
        } else if ("节日类".equals(Str(type))) {
            return "20";
        } else {
            return "NULL";
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

    private static String getPlayTime(String timeRanges) throws ParseException {
        if ("null".equals(timeRanges) || "请选择".equals(timeRanges) || "".equals(timeRanges)) {
            return "NULL";
        }
        String[] ranges = timeRanges.split(",");

        java.util.Date min = null;
        java.util.Date max = null;

        SimpleDateFormat format = new SimpleDateFormat("HH:mm");

        for(String range : ranges) {
            String[] times = range.split("-");
            java.util.Date start = format.parse(times[0]);
            Date end = format.parse(times[1]);

            if(min == null || start.before(min)) {
                min = start;
            }

            if(max == null || end.after(max)) {
                max = end;
            }
        }

        return "'"+format.format(min) + "--" + format.format(max)+"'";
    }
}
