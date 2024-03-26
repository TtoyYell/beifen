import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
public class TransferTradData {

    private static Map<String, String> areaMap;
    private static Map<String, String> depMap;
    private static Map<String, String> areIdFrmDep;
    private static List<Map<String, String>> userInf;
    private static Map<String, String> channelInf;

    public static void main(String[] args) throws Exception {

        Connection postgreConnection = getPostgreConnection();
        areaMap = getAreaMap(postgreConnection);
        depMap = getDepMap(postgreConnection);
        areIdFrmDep = getAreaIdFrmDep(postgreConnection);
        userInf = getUserInf(postgreConnection);
        channelInf = getChannelInf(postgreConnection);

        Connection mysqlConnection = getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();
        String sql = "SELECT fid ,\n" +
                " finput_date as '创建时间',\n" +
                "fmodify_time as '修改时间', \n" +
                "fmodify_user as '最终修改人', \n" +
                "fhandle_result as '反馈结果',\n" +
                "fhandle_enddate as '反馈日期',\n" +
                "finput_user as '派发方',\n" +
                "finput_userid as '派发方id',\n" +
                "fhandle_user as '反馈人员',\n" +
                "fadissue_date as '播出时段',\n" +
                "fadissue_times as '条目数',\n" +
                "fget_regulator as '接收单位',\n" +
                "fget_regulatorid as '接收单位id',\n" +
                "fpush_date as '下发时间',\n" +
                "fendtime as '处理截止时间',\n" +
                "fadname as '广告标题',\n" +
                "fadclass_name as '广告类别',\n" +
                "fadtypes as '广告类型',\n" +
                "ftarget_url as '链接入口',\n" +
                "ftarget_url as '落地页面icp',\n" +
                "ftarget_url as '广告落地页url',\n" +
                "fadsend_user as '发布平台',\n" +
                "fadowner_name as '广告主名称',\n" +
                "fadname as '广告内容',\n" +
                "fget_types as '线索来源',\n" +
                "fhandover_types as '交办方式',\n" +
                "1 as '违法级别',\n" +
                "fnum as '交办单号' ,\n" +
                "ftipoffs_unit as '移送或提供线索单位',\n" +
                "ftipoffs_user as '投诉或举报方',\n" +
                "ftipoffs_phone as '投诉或举报方联系方式',\n" +
                "case when fstatus = 10 then '待登记' when fstatus = 20 then '待处理' when fstatus = 30 then '核查中' when fstatus = 40 then '立案调查中' when fstatus =50 then '已办结' when -1 then '作废' end as '派发状态'\n" +
                "FROM `nj_clue`   where  fadtypes in('广播','广播电视','电视','报纸') and fstatus <> -1 ";
        ResultSet resultSet = stmt.executeQuery(sql);
        List<Map<String, Object>> mapList = resultSetToListMap(resultSet);
        for (Map<String, Object> map : mapList) {

            String fid = Str(map.get("fid"));
            Statement evidenceFlow = mysqlConnection.createStatement();
            String evidenceSql = " select \n" +
                    " a.fid as 'flowId',\n" +
                    " fcreatetime as '创建时间',\n" +
                    " a.fclueid as '系统相关广告id',\n" +
                    " fcreateperson as '处理人',\n" +
                    "case when a.fstatus = 10 then '待登记' when a.fstatus = 20 then '待处理' when a.fstatus = 30 then '核查中' when a.fstatus = 40 then '立案调查中' \n" +
                    "when a.fstatus =50 then '已办结' when -1 then '作废' end as '派发状态',\n" +
                    " b.fattachname,\n" +
                    " b.fattachurl,\n" +
                    " c.finput_regulator,\n" +
                    " c.finput_user\n" +
                    " from nj_clueflow a \n" +
                    " left join nj_cluefile b on a.fid = b.fflowid and a.fclueid = b.fclueid\n" +
                    " left join nj_clue c on a.fclueid = c.fid\n" +
                    " where a.fstatus = 10 and a.fclueid = "+fid;
            ResultSet evidenceRs = evidenceFlow.executeQuery(evidenceSql);
            List<Map<String,String>> evidenceList = new ArrayList<>();
            while (evidenceRs.next()) {
                Map<String,String> eviMap = new HashMap<>();
                String fattachurl = evidenceRs.getString("fattachurl");
                if (fattachurl !=null && !fattachurl.equals("")){
                    eviMap.put("url", fattachurl);
                    eviMap.put("name",evidenceRs.getString("fattachname"));
                    evidenceList.add(eviMap);
                }
            }

            // convert to json
            ObjectMapper mapper = new ObjectMapper();
            String evidenceJson = mapper.writeValueAsString(evidenceList);

            Statement insertStmt = mysqlConnection.createStatement();
            String insertSql = " INSERT INTO \"public\".\"trad_local_edition\" (\n" +
                    "    \"id\",\"created_time\",\"updated_time\",\"updated_by\",\"dispatch_status\"," +
                    "    \"deal_style\", \"ignored\",\"deal_at\",\"assigned_by\",\"accepted_by\"," +
                    "    \"assigned_at\",\"assigned_deadline\",\"deleted\", \"reviewed\",\"reviewed_by\",\"reviewed_at\",\"supervise\",\"deal_style2\",\n" +
                    "    \"name\",\"category_id\",\"advertiser_id\",\"channel_id\",\"first_illegal_time\",\"evidence\",\n" +
                    "    \"level\",\"laws\",\"description\",\"illegal_description\",\"clue\",\"file_path\",\n" +
                    "    \"tag_ids\",\"sp_tag_ids\",\"create_by\",\"clue_number\",\"clue_type\",\"clue_time\",\n" +
                    "    \"delay_time\",\"transfer_party\",\"complaint_party\",\"complaint_party_tel\",\n" +
                    "    \"accepted_department\",\"advertiser\",\"amount\",\"accepted_user\",\n" +
                    "    \"area_id\",\"is_timeout\",\"is_delay\",\"action_status\",\"user_deadline\"\n" +
                    " ) " +
                    " VALUES ("+fid+",'"+map.get("创建时间")+"', '"+map.get("修改时间")+"', "+getUserIdByName(Str(map.get("最终修改人")),Str(map.get("接收单位")))+", "+getDispatchStatus(Str(map.get("派发状态")))+"," +
                    " "+getDealStyle(Str(map.get("反馈结果")))+", 'f', "+ forDateNull(map.get("反馈日期")) +", "+getUserIdByName(Str(map.get("派发方")),"南京市市场监督管理局")+", "+getUserIdByName(Str(map.get("反馈人员")),Str(map.get("接收单位")))+", " +
                    " "+ forDateNull(map.get("下发时间")) +", "+ forDateNull(map.get("处理截止时间")) +", 'f', 't', NULL, " +
                    " NULL, 'f', NULL, '"+map.get("广告标题")+"', "+getCodeByClass(map.get("广告类别"))+", NULL, "+getChannelId(Str(map.get("发布平台")))+", '"+map.get("创建时间")+"', " +
                    " '"+evidenceJson+"', 1, '[\"1\"]','','','"+getClue(map.get("线索来源"))+"',NULL,NULL,NULL,"+getUserIdByName(Str(map.get("派发方")),"南京市市场监督管理局")+", '"+map.get("交办单号")+"', '"+getClueType(map.get("交办方式"))+"', "+forDateNull(map.get("下发时间"))+", NULL, " +
                    " '"+map.get("移送或提供线索单位")+"', '"+map.get("投诉或举报方")+"', '"+map.get("投诉或举报方联系方式")+"'," +
                    " '"+getAcceptedDep(map.get("接收单位"))+"','"+map.get("发布平台")+"',"+map.get("条目数")+", " +
                    " "+getUserIdByName(Str(map.get("反馈人员")),Str(map.get("接收单位")))+", "+getAreaID(map.get("接收单位"))+", 'f', NULL, "+getActionStatus(Str(map.get("派发状态")))+", NULL);\n ";

            System.out.println(insertSql);
            if (false) {
                insertStmt.execute(insertSql);
            }

            // 导入流程
            Statement stmtFlow = mysqlConnection.createStatement();
            String flowSql = " select \n" +
                    " a.fid as 'flowId',\n" +
                    " fcreatetime as '创建时间',\n" +
                    " a.fclueid as '系统相关广告id',\n" +
                    " fcreateperson as '处理人',\n" +
                    "case when a.fstatus = 10 then '待登记' when a.fstatus = 20 then '待处理' when a.fstatus = 30 then '核查中' when a.fstatus = 40 then '立案调查中' \n" +
                    "when a.fstatus =50 then '已办结' when -1 then '作废' end as '派发状态',\n" +
                    " b.fattachname,\n" +
                    " b.fattachurl,\n" +
                    " c.finput_regulator,\n" +
                    " c.finput_user\n" +
                    " from nj_clueflow a \n" +
                    " left join nj_cluefile b on a.fid = b.fflowid and a.fclueid = b.fclueid\n" +
                    " left join nj_clue c on a.fclueid = c.fid\n" +
                    " where a.fstatus <> 10 and a.fclueid = "+fid;
            ResultSet flowRs = stmtFlow.executeQuery(flowSql);
            List<Map<String, Object>> flowList = resultSetToListMap(flowRs);
            flowList = merge(flowList);

            for (Map<String, Object> flow : flowList) {
                // 如果办结有note
                String note = "";
                if (Str(map.get("反馈结果")).contains("其他") && "已办结".equals(Str(map.get("派发状态")))
                        || Str(map.get("反馈结果")).equals("责令停止发布") && "已办结".equals(Str(map.get("派发状态")))) {
                    if (Str(map.get("反馈结果")).equals("责令停止发布")) {
                        note = "责令暂停发布";
                    } else {
                        note = getContentInBrackets(Str(map.get("反馈结果")));
                    }
                }

                Statement insertFlowStmt = mysqlConnection.createStatement();
                String insertFlowSql = " INSERT INTO \"public\".\"trad_action_local\" ( " +
                        " \"created_time\"," +
                        " \"updated_time\"," +
                        " \"updated_by\"," +
                        " \"conn_id\"," +
                        " \"dispatch_status\"," +
                        " \"archives\"," +
                        " \"assigned_by\"," +
                        " \"assigned_by_info\"," +
                        " \"accepted_by\"," +
                        " \"accepted_by_info\"," +
                        " \"note\"," +
                        " \"supervise\"," +
                        " \"clue\" " +
                        " ) " +
                        " VALUES " +
                        " (" +
                        "  '"+flow.get("创建时间")+"'," +
                        "  NULL," +
                        "  NULL," +
                        "  "+flow.get("系统相关广告id")+"," +
                        "  "+getDispatchStatus(Str(flow.get("派发状态")))+"," +
                        "  '"+getFileJson(flow.get("file"))+"'," +
                        "  "+getUserIdByName(Str(flow.get("finput_user")),"南京市市场监督管理局")+"," +
                        "  '"+getUserJson(Str(flow.get("finput_user")),"南京市市场监督管理局")+"'," +
                        "  "+getUserIdByName(Str(flow.get("处理人")), Str(map.get("接收单位")))+"," +
                        "  '"+getUserJson(Str(flow.get("处理人")), Str(map.get("接收单位")))+"'," +
                        "  '"+note+"'," +
                        "  'f'," +
                        "  NULL " +
                        " );";
                System.out.println(insertFlowSql);
                if (false) {
                    insertFlowStmt.execute(insertFlowSql);
                }
            }

        }

        stmt.close();
        // 关闭连接
        mysqlConnection.close();

    }

    private static String getChannelId(String pingtai) {
        if (channelInf.get(pingtai)!= null && !"".equals(channelInf.get(pingtai))) {
            return channelInf.get(pingtai);
        } else {
            if(pingtai.contains("广播")) {
                return "1";
            }
            if  (pingtai.contains("电视")) {
                return "4";
            }
            if  (pingtai.contains("报纸")) {
                return "5";
            }
            return "3";
        }
    }


    private static String forDateNull(Object value) {
        if (value==null || "".equals(value)) {
            return "NULL";
        } else {
            return "'"+Str(value)+"'";
        }
    }
    private static String forNull(Object value) {
        if (value==null || "".equals(value)) {
            return "NULL";
        } else {
            return Str(value);
        }
    }

    private static String getFileJson(Object file) throws JsonProcessingException {
        if (file == null) {
            return "null";
        }
        String fileString = Str(file);
        List<Map<String,String>> jsonList = new ArrayList<>();
        for (String s : fileString.split(";")) {
            Map<String,String> map = new HashMap<>();
            String url = s.split("->")[0];
            String name = s.split("->")[1];
            map.put("url",url);
            map.put("name",name);
            jsonList.add(map);
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(jsonList);
    }

    private static Map<String, Object> findTwoId(List<Map<String, Object>> flowList, String flowId, String conId) {
        Map<String, Object> resMap = new HashMap<>();
        for (Map<String, Object> map : flowList) {
            if (map.get("flowId").equals(flowId) && map.get("系统相关广告id").equals(conId)) {
                resMap = map;
            }
        }
        return resMap;
    }

    private static List<Map<String,String>> getUserInf(Connection postgreConnection) throws SQLException {
        List<Map<String,String>> res = new ArrayList<>();
        Statement statement = postgreConnection.createStatement();
        String sql = " select a.id,a.name,a.department_id,b.name as depName from customer a \n" +
                "left join department b on a.department_id = b.id ";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            Map<String, String> map = new HashMap<>();
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String departmentId = resultSet.getString("department_id");
            String departName = resultSet.getString("depName");
            map.put("id",id);
            map.put("name",name);
            map.put("department_id",departmentId);
            map.put("department_name",departName);
            res.add(map);
        }
        return res;
    }

    private static String getUserJson(String finputUser,String dep) throws JsonProcessingException {
        finputUser = removeBracketContent(finputUser);
        if (finputUser.equals("南京演示")) {
            finputUser = "陈慧";
        }
        if (finputUser.equals("南京市局")) {
            finputUser = "陈慧";
        }
        if (finputUser.equals("倪蒙")) {
            finputUser = "陈慧";
        }
        if (finputUser.equals("夏妍")) {
            finputUser = "万华";
        }
        if (finputUser.equals("唐传众")) {
            finputUser = "金海涛";
        }
        if (finputUser.contains("王洁霏")) {
            finputUser = "王凡凡";
        }
        if (finputUser.contains("建邺区局")) {
            finputUser = "王凡凡";
        }
        if (finputUser.contains("邱巧珍")) {
            finputUser = "涂磊";
        }
        if (finputUser.contains("陶孝巧")) {
            finputUser = "汪婷";
        }
        if (finputUser.contains("浦口区局")) {
            finputUser = "汪传林";
        }
        if (finputUser.contains("秦淮区局")) {
            finputUser = "马珑珈";
        }

        if (finputUser== null || "".equals(finputUser) || "null".equals(finputUser)) {
            dep = dep.replace("南京","").replace("经济开发区市场监督管理局","经济技术开发区市场监督管理局");
            if (dep.equals("南京市市场监督管理局")) {
                finputUser = "陈慧";
            }
            if (dep.equals("玄武区市场监督管理局")) {
                finputUser = "万华";
            }
            if (dep.equals("秦淮区市场监督管理局")) {
                finputUser = "马珑珈";
            }
            if (dep.equals("建邺区市场监督管理局")) {
                finputUser = "王凡凡";
            }
            if (dep.equals("鼓楼区市场监督管理局")) {
                finputUser = "谢刚";
            }
            if (dep.equals("栖霞区市场监督管理局")) {
                finputUser = "李传伟";
            }
            if (dep.equals("雨花台区市场监督管理局")) {
                finputUser = "金海涛";
            }
            if (dep.equals("江宁区市场监督管理局")) {
                finputUser = "汪婷";
            }
            if (dep.equals("浦口区市场监督管理局")) {
                finputUser = "邢骏";
            }
            if (dep.equals("六合区市场监督管理局")) {
                finputUser = "章荣";
            }
            if (dep.equals("溧水区市场监督管理局")) {
                finputUser = "涂磊";
            }
            if (dep.equals("高淳区市场监督管理局")) {
                finputUser = "丁超";
            }
            if (dep.equals("江北新区市场监督管理局")) {
                finputUser = "韩丽";
            }
            if (dep.equals("经济技术开发区市场监督管理局")) {
                finputUser = "肖红军";
            }
        }
        Map<String, String> userMap = new HashMap<>();
        for (Map<String, String> map : userInf) {
            if (map.get("name").equals(finputUser)) {
                userMap = map;
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(userMap);
    }

    private static Map<String, String> getAreaIdFrmDep(Connection postgreConnection) throws SQLException {
        Map<String,String> res = new HashMap<>();
        Statement statement = postgreConnection.createStatement();
        String sql = " select area_id,name from department ";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            String id = resultSet.getString("area_id");
            String name = resultSet.getString("name");
            res.put(name,id);
        }
        return res;
    }


    private static Map<String, String> getChannelInf(Connection postgreConnection) throws SQLException {
        Map<String,String> res = new HashMap<>();
        Statement statement = postgreConnection.createStatement();
        String sql = " select id,name from trad_channel ";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            res.put(name,id);
        }
        return res;
    }

    private static Map<String, String> getDepMap(Connection postgreConnection) throws SQLException {
        Map<String,String> res = new HashMap<>();
        Statement statement = postgreConnection.createStatement();
        String sql = " select id,name from department ";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            res.put(name,id);
        }
        return res;
    }

    private static Map<String, String> getAreaMap(Connection postgreConnection) throws SQLException {
        Map<String,String> res = new HashMap<>();
        Statement statement = postgreConnection.createStatement();
        String sql = " select id,name from area ";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            res.put(name,id);
        }
        return res;
    }

    private static String getAcceptedDep(Object acepDep) {
        return depMap.get(Str(acepDep).replace("南京",""));
    }

    private static String getAreaID(Object dep) {
        return areIdFrmDep.get(Str(dep).replace("南京",""));
    }

    private static String getClueType(Object clueType) {
        if (Str(clueType).contains("平台") && Str(clueType).contains("寄送")) {
            return "平台,寄送";
        } else if (Str(clueType).contains("平台") && !Str(clueType).contains("寄送")) {
            return "平台";
        } else if (!Str(clueType).contains("平台") && Str(clueType).contains("寄送")) {
            return "寄送";
        } else return "";
    }

    private static String getClue(Object clue) {
        Map<String,String> clueMap = new HashMap<>();
        clueMap.put("","其他（）");
        clueMap.put("上级监测（其他平台）","其他（其他上级平台监测）");
        clueMap.put("上级监测（总局互联网广告监测平台）","总局平台（互联网）");
        clueMap.put("上级监测（总局互联网监测平台）；","总局平台（互联网）");
        clueMap.put("上级监测（总局传统媒体广告监测平台）","总局平台（互联网）");
        clueMap.put("上级监测（总局移动互联网监测平台）；","总局平台（互联网）");
        clueMap.put("上级监测（省局三位一体广告监测平台）；","省局平台派发（登记）");
        clueMap.put("上级监测（省局三位一体监管平台）；","省局平台派发（登记）");
        clueMap.put("上级监测（省局互联网监测平台）；","省局平台派发（登记）");
        clueMap.put("举报","投诉举报");
        clueMap.put("其他","其他（）");
        clueMap.put("其他单位移送","其他（其他单位移送）");
        clueMap.put("其他（市局网络舆情监测）","其他（市局网络舆情监测）");
        clueMap.put("其他（校外培训舆情）","其他（校外培训舆情）");
        clueMap.put("北京市海淀区市场监管局移送","其他（北京市海淀区市场监管局移送）");
        clueMap.put("外单位移送","其他（外单位移送）");
        clueMap.put("总局交办","总局平台（互联网）");
        clueMap.put("总局督办","总局平台（互联网）");
        clueMap.put("滁州局移送","其他（滁州局移送）");
        clueMap.put("王海来信举报","其他（王海来信举报）");
        clueMap.put("直接投诉或举报","投诉举报");
        clueMap.put("省局交办","省局平台派发（登记）");
        clueMap.put("省局医疗器械生产监管处","其他（省局医疗器械生产监管处）");
        clueMap.put("省局转办","省局平台派发（登记）");
        clueMap.put("移送","其他（移送）");
        clueMap.put("自行发现","其他（自行发现）");
        clueMap.put("自行发行","其他（自行发现）");
        return clueMap.get(Str(clue));
    }

    private static String getTypeFromFadTypes(Object fadTypes) {
        Map<String, String> typeMap = new HashMap<>();
        typeMap.put("360搜索","0");typeMap.put("360浏览器","0");typeMap.put("APP","2");typeMap.put("PC网站","1");
        typeMap.put("pc门户","1");typeMap.put("PC门户网站","1");typeMap.put("互联网","1");typeMap.put("互联网PC端","1");
        typeMap.put("互联网PC端；","1");typeMap.put("互联网移动端-APP","2");typeMap.put("互联网移动端-公众号","3");typeMap.put("互联网移动端-其他","2");
        typeMap.put("互联网移动端-移动H5","1");typeMap.put("互联网移动端；","2");typeMap.put("公众号","3");typeMap.put("大众点评","2");
        typeMap.put("大蓝鲸精选","2");typeMap.put("天猫","4");typeMap.put("小程序","2");typeMap.put("小红书","2");
        typeMap.put("广告搜索","0");typeMap.put("店堂广告","");typeMap.put("微信","2");typeMap.put("微信公众号","3");
        typeMap.put("微信小程序","2");typeMap.put("微博","2");typeMap.put("快手","2");typeMap.put("快手直播-百度","5");
        typeMap.put("抖音","2");typeMap.put("抖音直播-百度","5");typeMap.put("拼多多","2");typeMap.put("搜狗","2");
        typeMap.put("搜狗搜索","0");typeMap.put("新浪微博","2");typeMap.put("普通食品类","4");typeMap.put("淘宝","4");
        typeMap.put("淘宝直播-百度","5");typeMap.put("电商平台","4");typeMap.put("百度","0");typeMap.put("百度APP","2");
        typeMap.put("移动APP","2");typeMap.put("移动搜索引擎","0");typeMap.put("移动网页","1");typeMap.put("移动轻应用","2");
        typeMap.put("美团","2");typeMap.put("自媒体号","3");typeMap.put("自建网站","1");typeMap.put("门户网站","1");
        typeMap.put("阿里巴巴","1");
        return typeMap.get(Str(fadTypes));
    }

    private static String getCodeByClass(Object adClass) {
        Map<String, String> categoryMap = new HashMap<>();
        categoryMap.put("一般服务","20");categoryMap.put("乳制品","109");categoryMap.put("互联网服务","16");categoryMap.put("互联网服务类","16");
        categoryMap.put("交通产品","10");categoryMap.put("交通产品类","10");categoryMap.put("休闲娱乐服务","192");categoryMap.put("保健用品","137");
        categoryMap.put("保健食品","119");categoryMap.put("保健食品类","119");categoryMap.put("信息服务","15");categoryMap.put("其他","210");
        categoryMap.put("其他商业招商投资","178");categoryMap.put("其他商品","157");categoryMap.put("其他教育培训服务","181");categoryMap.put("其他普通商品","157");
        categoryMap.put("其他普通服务","204");categoryMap.put("其他普通食品","210");categoryMap.put("其他电器","129");categoryMap.put("其他知识产品","136");
        categoryMap.put("其他类","210");categoryMap.put("其他类-","210");categoryMap.put("其他类型广告","210");categoryMap.put("其它","210");
        categoryMap.put("农兽药","153");categoryMap.put("农林渔牧物资","154");categoryMap.put("农资","154");categoryMap.put("冲饮食品","112");
        categoryMap.put("办公学习用品","146");categoryMap.put("化妆","3");categoryMap.put("化妆品","3");categoryMap.put("化妆品类","3");
        categoryMap.put("化妆品类-普通化妆品","104");categoryMap.put("化妆品类-特殊化妆品","105");categoryMap.put("医疗器械","2");categoryMap.put("医疗器械类","2");
        categoryMap.put("医疗服务","13");categoryMap.put("医疗服务/医疗美容服务","159");categoryMap.put("医疗服务/医疗诊疗服务","158");categoryMap.put("医疗服务类","13");
        categoryMap.put("医疗服务类-医疗诊疗服务","158");categoryMap.put("医疗服务（医疗美容）","159");categoryMap.put("医疗用品","2");categoryMap.put("医疗美容","159");
        categoryMap.put("医疗美容服务","159");categoryMap.put("医疗诊疗服务","158");categoryMap.put("南京华美医院","158");categoryMap.put("卫生用品","2");
        categoryMap.put("厨卫电器","126");categoryMap.put("咨询服务","201");categoryMap.put("商业招商投资","17");categoryMap.put("商业招商投资类","17");
        categoryMap.put("商业招商投资类-","17");categoryMap.put("商务服务","204");categoryMap.put("商品房","106");categoryMap.put("图书","157");
        categoryMap.put("图书音像出版物","157");categoryMap.put("培训","180");categoryMap.put("奶粉","111");categoryMap.put("宠物产品","156");
        categoryMap.put("家具家私","144");categoryMap.put("导向问题广告","210");categoryMap.put("小家电产品","128");categoryMap.put("建材五金","151");
        categoryMap.put("形象宣传","205");categoryMap.put("性用品","213");categoryMap.put("房地产","4");categoryMap.put("房地产类","4");
        categoryMap.put("招商","176");categoryMap.put("招商投资","17");categoryMap.put("招商投资金融保险","168");categoryMap.put("收藏品","149");
        categoryMap.put("教育","179");categoryMap.put("教育、培训","18");categoryMap.put("教育培训","18");categoryMap.put("教育培训服务","18");
        categoryMap.put("教育培训服务类","18");categoryMap.put("教育培训服务类-","18");categoryMap.put("教育培训服务类-培训","18");categoryMap.put("教育培训类","18");
        categoryMap.put("文体娱乐产品","152");categoryMap.put("旅游服务","187");categoryMap.put("日用品","210");categoryMap.put("普通化妆品","105");
        categoryMap.put("普通商品","12");categoryMap.put("普通商品/其他普通商品","157");categoryMap.put("普通商品/建材五金","151");categoryMap.put("普通商品类","12");
        categoryMap.put("普通商品类-农兽药","153");categoryMap.put("普通商品类-钟表眼镜","148");categoryMap.put("普通服务","20");categoryMap.put("普通服务/其他普通服务","204");
        categoryMap.put("普通服务/装饰装潢服务","194");categoryMap.put("普通服务类","20");categoryMap.put("普通食品","5");categoryMap.put("普通食品类","5");
        categoryMap.put("服务","20");categoryMap.put("服装","143");categoryMap.put("服饰箱包","143");categoryMap.put("机动车","130");categoryMap.put("江鲜","5");
        categoryMap.put("汽车","132");categoryMap.put("洁齿用品","139");categoryMap.put("消毒产品","138");categoryMap.put("消毒类产品","138");
        categoryMap.put("清洁用品","141");categoryMap.put("特效加盟","177");categoryMap.put("特殊化妆品","105");categoryMap.put("特许加盟","177");
        categoryMap.put("电器","9");categoryMap.put("电子产品","9");categoryMap.put("男用延时喷剂","213");categoryMap.put("知识产品类","11");
        categoryMap.put("知识产品类-音像印刷出版物","11");categoryMap.put("移动通信服务","170");categoryMap.put("空气调节设备","124");categoryMap.put("纺织品","145");
        categoryMap.put("美容美发美体","193");categoryMap.put("美容美发美体（除医疗美容）","193");categoryMap.put("药品","1");categoryMap.put("药品/非处方药","102");
        categoryMap.put("药品类","1");categoryMap.put("装饰装潢服务","194");categoryMap.put("证券","14");categoryMap.put("通信设备","125");
        categoryMap.put("酒","122");categoryMap.put("酒类","122");categoryMap.put("野生动物","210");
        categoryMap.put("金融","14");categoryMap.put("金融投资类","14");categoryMap.put("金融服务","14");categoryMap.put("金融服务类","168");
        categoryMap.put("钟表眼镜","148");categoryMap.put("零售","210");categoryMap.put("非处方药","102");categoryMap.put("非处方药/保健食品","119");
        categoryMap.put("音像印刷出版物","135");categoryMap.put("食品","5");categoryMap.put("食用油脂","5");categoryMap.put("餐饮服务","189");
        categoryMap.put("饮料","110");categoryMap.put("饮料和饮料添加剂","110");categoryMap.put("饲料和饲料添加剂","110");categoryMap.put("首饰","147");
        if (categoryMap.get(Str(adClass)) == null) {
            return "210";
        }
        return categoryMap.get(Str(adClass));
    }

    @Deprecated
    private static String getDealStyle2(String result) {
        if ("移送司法机关".equals(result)) {

        } else if ("行政处罚".equals(result)) {

        } else if ("行政处罚".equals(result)) {

        }
        return "";
    }

    private static String getDealStyle(String result) {
        if (result.equals("")) {
            return "0";
        } else if (result.equals("不予立案")) {
            return "4";
        } else if (result.equals("移送司法机关")) {
            return "3";
        } else if (result.equals("行政处罚")) {
            return "2";
        } else if (result.equals("责令停止发布")) {
            return "5";
        } else if (result.contains("不予处罚") || result.contains("不处罚")){
            return "1";
        } else {
            // 其他情况
            return "5";
        }
    }

    private static String getUserIdByName(String userName,String dep) {
        userName = removeBracketContent(userName);

        if (userName.equals("南京演示")) {
            userName = "陈慧";
        }
        if (userName.equals("南京市局")) {
            userName = "陈慧";
        }
        if (userName.equals("倪蒙")) {
            userName = "陈慧";
        }
        if (userName.equals("夏妍")) {
            userName = "万华";
        }
        if (userName.equals("唐传众")) {
            userName = "金海涛";
        }
        if (userName.contains("建邺区局")) {
            userName = "王凡凡";
        }
        if (userName.contains("王洁霏")) {
            userName = "王凡凡";
        }
        if (userName.contains("邱巧珍")) {
            userName = "涂磊";
        }
        if (userName.contains("陶孝巧")) {
            userName = "汪婷";
        }
        if (userName.contains("浦口区局")) {
            userName = "汪传林";
        }
        if (userName.contains("秦淮区局")) {
            userName = "马珑珈";
        }

        if (userName== null || "".equals(userName) || "null".equals(userName)) {
            dep = dep.replace("南京","").replace("经济开发区市场监督管理局","经济技术开发区市场监督管理局");
            if (dep.equals("南京市市场监督管理局")) {
                userName = "陈慧";
            }
            if (dep.equals("玄武区市场监督管理局")) {
                userName = "万华";
            }
            if (dep.equals("秦淮区市场监督管理局")) {
                userName = "马珑珈";
            }
            if (dep.equals("建邺区市场监督管理局")) {
                userName = "王凡凡";
            }
            if (dep.equals("鼓楼区市场监督管理局")) {
                userName = "谢刚";
            }
            if (dep.equals("栖霞区市场监督管理局")) {
                userName = "李传伟";
            }
            if (dep.equals("雨花台区市场监督管理局")) {
                userName = "金海涛";
            }
            if (dep.equals("江宁区市场监督管理局")) {
                userName = "汪婷";
            }
            if (dep.equals("浦口区市场监督管理局")) {
                userName = "邢骏";
            }
            if (dep.equals("六合区市场监督管理局")) {
                userName = "章荣";
            }
            if (dep.equals("溧水区市场监督管理局")) {
                userName = "涂磊";
            }
            if (dep.equals("高淳区市场监督管理局")) {
                userName = "丁超";
            }
            if (dep.equals("江北新区市场监督管理局")) {
                userName = "韩丽";
            }
            if (dep.equals("经济技术开发区市场监督管理局")) {
                userName = "肖红军";
            }
        }

        String id = "";
        for (Map<String, String> map : userInf) {
            if (userName.equals(map.get("name"))){
                id = map.get("id");
            }
        }

        return id;
    }

    private static String getDispatchStatus(String paiFazt) {
        if (paiFazt.equals("待处理")) {
            return "1";
        } else if (paiFazt.equals("核查中")) {
            return "7";
        } else if (paiFazt.equals("立案调查中")) {
            return "8";
        } else if (paiFazt.equals("已办结")) {
            return "2";
        } else if (paiFazt.equals("作废")) {
            return "99";
        } else {
            return "";
        }
    }
    private static String getActionStatus(String paiFazt) {
        if (paiFazt.equals("待处理")) {
            return "0";
        } else if (paiFazt.equals("核查中")) {
            return "1";
        } else if (paiFazt.equals("立案调查中")) {
            return "2";
        } else if (paiFazt.equals("已办结")) {
            return "2";
        } else if (paiFazt.equals("作废")) {
            return "99";
        } else {
            return "";
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
        String url = "jdbc:mysql://192.168.172.103:3306/test";
        String user = "root";
        String password = "Y@cp3winer";

        // 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
    public static String getContentInBrackets(String str) {
        int start = str.indexOf("（");
        int end = str.indexOf("）");
        if(start != -1 && end != -1 && end > start) {
            return str.substring(start + 1, end);
        } else {
            return "";
        }
    }

    public static List<Map<String, Object>> merge(List<Map<String, Object>> list) {

        List<Map<String, Object>> result = new ArrayList<>();

        for(Map<String, Object> map : list) {
            String id1 = map.get("flowId").toString();
            String id2 = map.get("系统相关广告id").toString();
            boolean merged = false;
            for(Map<String, Object> resultMap : result) {
                if(id1.equals(Str(resultMap.get("flowId"))) && id2.equals(Str(resultMap.get("系统相关广告id")))) {
                    String url = resultMap.get("fattachurl").toString();
                    String name = resultMap.get("fattachname").toString();
                    String file = url + "->" +name;

                    file += ";" + resultMap.get("file");
                    resultMap.put("file", file);
                    merged = true;
                    break;
                }
            }

            if(!merged) {
                if (map.get("fattachurl")!=null) {
                    String url = map.get("fattachurl").toString();
                    String name = map.get("fattachname").toString();
                    String file = url + "->" +name;
                    map.put("file", file);
                }
                result.add(map);
            }
        }

        return result;
    }
    public static String extractHttpPart(String text) {
        int start = text.indexOf("http");
        if(start != -1) {
            int end = text.length();
            return text.substring(start, end);
        } else {
            return "";
        }
    }

    public static String removeBracketContent(String text) {
        int start;
        while((start = text.indexOf("（")) != -1) {
            int end = text.indexOf("）", start);
            if(end != -1) {
                text = text.substring(0, start) + text.substring(end + 1);
            }
        }

        return text;
    }
}
