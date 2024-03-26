import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/22 10:24
 */
public class TraDataTranfer {

    public static void main(String[] args) throws Exception {

        String targetFile = "C:\\Users\\Admin\\Desktop\\数据3.sql";
        FileWriter writer = new FileWriter(targetFile);

        String targetFile2 = "C:\\Users\\Admin\\Desktop\\数据adv.sql";
        FileWriter writer2 = new FileWriter(targetFile2);

        Connection postgreConnection = getPostgreConnection();

        Connection sqlServerConnection = getSqlServerConnection();

        Statement stmt = sqlServerConnection.createStatement();

        String sql0 = " select  SCCJDM as 广告主代码,SCCJ as 广告主名称 from SCCJ ";
        ResultSet resultSet0 = stmt.executeQuery(sql0);
        List<Map<String, Object>> mapList0 = resultSetToListMap(resultSet0);
        for (Map<String, Object> map : mapList0) {
            Statement insertStmt = postgreConnection.createStatement();
            String advSql = " INSERT INTO \"public\".\"trad_advertiser\" (\"id\", \"name\")" +
                    "VALUES ("+map.get("广告主代码")+", '"+map.get("广告主名称")+"'); ";
            System.out.println(advSql);
            writer2.write(advSql+"\n");
            writer2.flush();
            if (false) {
                insertStmt.execute(advSql);
            }
        }

        String sql = " select " +
                " a.GGBH as 广告编号," +
                " b.GGYBBH as 广告样本编号," +
                " GGMC as 广告名称," +
                " PP as 品牌," +
                " SCCJDM as 广告主代码, " +
                " GGLB as 广告内容类别, " +
                " LRSJ  as 广告录入时间," +
                " c.MTMC as 媒体名称," +
                " case when c.MTLBDM = 1 then '报纸' when c.MTLBDM = 3 then '电视' when c.MTLBDM = 4 then '广播' end as 媒体类别, " +
                " b.FXRQ as 发行日期, " +
                " WFLXDM as 违法情况代码, " +
                " SXWFNR as 违法行为表现, " +
                " e.BM as 版面, " +
                " e.SL as 平面媒体数量, " +
                " b.WFBXDMS as 违法广告代码 " +
                " from GG a " +
                " left join GGYB b on a.GGBH = b.GGBH " +
                " left join GGLB d on a.GGLBDM = d.GGLBDM " +
                " left join MT c on b.MTDM = c.MTDM " +
                " left join BZGGFB e on e.GGYBBH = b.GGYBBH where b.GGYBBH is not null ";
        System.out.println(sql);
        Statement stmtEdit = sqlServerConnection.createStatement();
        ResultSet resultSetEdit = stmtEdit.executeQuery(sql);
        List<Map<String, Object>> mapList = resultSetToListMap(resultSetEdit);

        String sql2 = " select " +
                " a.GGYBBH as 广告样本编号," +
                " YBCD as 样本长度," +
                " c.BM as 版面," +
                " c.SL as 数量," +
                " KSSJ as 流媒体违法时间," +
                " c.FXRQ as 平面媒体违法日期," +
                " b.DJSJ as 流媒体录入时间," +
                " c.DJSJ as 平面媒体录入时间," +
                " d.MTMC as 媒体名称," +
                " case when d.MTLBDM = 1 then '报纸' when d.MTLBDM = 3 then '电视' when d.MTLBDM = 4 then '广播' end as 媒体类别" +
                " from  GGYB a " +
                " left join GGFB b on a.GGYBBH = b.GGYBBH" +
                " left join BZGGFB c on a.GGYBBH = c.GGYBBH" +
                " left join MT d on d.MTDM = b.MTDM ";
        Statement stmtIgg = sqlServerConnection.createStatement();
        ResultSet resultIgg = stmtIgg.executeQuery(sql2);
        List<Map<String, Object>> mapList2 = resultSetToListMap(resultIgg);

        for (int i = 0; i < mapList.size(); i++) {
            Map<String, Object> map = mapList.get(i);
            Statement insertStmt = postgreConnection.createStatement();
            String insertSql = " INSERT INTO \"public\".\"trad_edition\"" +
                    " (\"id\", \"created_at\", \"dispatch_status\", \"deal_style\", \"ignored\", \"deal_at\", " +
                    " \"assigned_by\", \"accepted_by\", \"assigned_at\", \"assigned_deadline\", \"deleted\", \"reviewed\", \"reviewed_by\", \"reviewed_at\", " +
                    " \"supervise\", \"deal_style2\", \"name\", \"size\", \"category_id\", \"type\", \"advertiser_id\", \"channel_id\", " +
                    " \"evidence\", \"level\",\"banner_id\", \"laws\", \"description\", \"illegal_description\", \"created_by\", \"original_edition_id\", \"is_updated\", " +
                    " \"first_illegal_time\", \"view_count\", \"source_id\", \"start_page\", \"end_page\", \"extend_phone\", \"tag_ids\", \"sp_tag_ids\", " +
                    " \"assignment_to\", \"allow_distribute\", \"reviewed_sp_tag\", \"production_time\", \"merge_id\", \"reviewed_back\", \"timeout\", " +
                    " \"is_timeout\", \"is_delay\", \"action_status\", \"user_deadline\")" +
                    " VALUES ("+map.get("广告样本编号")+",'"+ map.get("广告录入时间")+"', 0, 0, 'f', NULL, " +
                    " NULL, NULL, NULL, NULL, 'f', 'f', NULL, NULL, " +
                    " 'f', NULL, '"+Str(map.get("广告名称")).replace("'","''")+"', '100', "+getCateGoryId(map.get("广告内容类别"))+", "+getAdvType(map.get("媒体类别"))+", "+map.get("广告主代码")+", "+getMeidaCode(map.get("媒体名称"))+", " + //TODO size需要更新
                    " '[]', "+getAdLevel(map.get("违法情况代码"))+", "+getBannerId(map.get("版面"))+", '"+getLaws(map.get("违法广告代码"))+"', '', '"+NullToBlank(map.get("违法行为表现"))+"', 2535, NULL, 0, " +
                    " '"+map.get("发行日期")+"', NULL, NULL, NULL, NULL, NULL, NULL, NULL, " +// TODO 首次违法时间需更新
                    " NULL,'f', 't', '"+map.get("广告录入时间")+"', NULL, NULL, NULL, " +
                    " 'f', NULL, 0, 'f' );";
            System.out.println(insertSql);
            writer.write(insertSql+"\n");
            writer.flush();

            if (false) {
                insertStmt.execute(insertSql);
            }
        }

        for (int i = 0; i < mapList2.size(); i++) {
            Map<String, Object> map = mapList2.get(i);
            Statement insertStmt = postgreConnection.createStatement();
            String insertSql = " INSERT INTO \"public\".\"trad_illegal\" " +
                    " (\"created_at\", \"edition_id\", " +
                    " \"length\", \"position\", \"illegal_at\", \"channel_id\", " +
                    " \"created_by\", \"amount\", \"original_illegal_id\", \"type\", \"source_id\") VALUES " +
                    " ('"+getTime(map.get("流媒体录入时间"),map.get("平面媒体录入时间"))+"', "+map.get("广告样本编号")+", " +
                    "  "+map.get("样本长度")+", NULL, '"+getTime(map.get("流媒体违法时间"),map.get("平面媒体违法日期"))+"', "+getMeidaCode(map.get("媒体名称"))+", " +
                    "  2535, "+getAmount(map.get("数量"))+", NULL, "+getAdvType(map.get("媒体类别"))+", NULL); ";
            System.out.println(insertSql);
            writer.write(insertSql+"\n");
            writer.flush();

            if (false) {
                insertStmt.execute(insertSql);
            }
        }

        stmt.close();
        // 关闭连接
        sqlServerConnection.close();

    }

    private static String getAmount(Object amount) {
        if (amount == null) {
            return "1";
        } else {
            return Str(amount);
        }
    }

    private static String getTime(Object liu, Object ping) {
        if (liu == null) {
            return Str(ping);
        } else {
            return Str(liu);
        }
    }


    private static String NullToBlank(Object area) {
        if (area == null) {
            return "";
        } else return Str(area).replace("'","''");
    }


    public static String getLaws(Object laws) {//YP8;0YP13 ["YLQX1"]
        if (laws == null) {
            return "[]";
        } else {
            String res = "[";
            String lawsStr = Str(laws);
            String[] strings = lawsStr.split(";");
            for (int i = 0; i < strings.length; i++) {
                res = res + "\""+strings[i]+"\"";
                if (i != strings.length-1) {
                    res = res + ", ";
                }
            }
            res = res + "]";
            return res;
        }
    }

    private static String getBannerId(Object banner) {
        if (banner == null) {
            return "NULL";
        } else {
            Map<String, String> bannerMap = new HashMap<>();
            bannerMap.put("A16","1");
            bannerMap.put("A23","2");
            bannerMap.put("A4-A5","3");
            bannerMap.put("A15-A16","4");
            bannerMap.put("B3","5");
            bannerMap.put("A6-A8","6");
            bannerMap.put("T12-T13","7");
            bannerMap.put("A3","8");
            bannerMap.put("B8","9");
            bannerMap.put("A7","10");
            bannerMap.put("B6","11");
            bannerMap.put("12-14","12");
            bannerMap.put("A19","13");
            bannerMap.put("2","14");
            bannerMap.put("9-10","15");
            bannerMap.put("NJ03","16");
            bannerMap.put("T12","17");
            bannerMap.put("YZ14","18");
            bannerMap.put("AII03","19");
            bannerMap.put("T21","20");
            bannerMap.put("10-14","21");
            bannerMap.put("T15","22");
            bannerMap.put("A6-A11","23");
            bannerMap.put("3-4","24");
            bannerMap.put("T1","25");
            bannerMap.put("B-C","26");
            bannerMap.put("T16","27");
            bannerMap.put("A07-A09","28");
            bannerMap.put("A10-11","29");
            bannerMap.put("T20","30");
            bannerMap.put("A06","31");
            bannerMap.put("T24","32");
            bannerMap.put("A20","33");
            bannerMap.put("8-12","34");
            bannerMap.put("SZ05","35");
            bannerMap.put("A2","36");
            bannerMap.put("A5-A8","37");
            bannerMap.put("15","38");
            bannerMap.put("A","39");
            bannerMap.put("B2-B4","40");
            bannerMap.put("A4","41");
            bannerMap.put("A9","42");
            bannerMap.put("a4","43");
            bannerMap.put("A09","44");
            bannerMap.put("B12","45");
            bannerMap.put("A08-A09","46");
            bannerMap.put("B08","47");
            bannerMap.put("05","48");
            bannerMap.put("A21","49");
            bannerMap.put("B1-B4","50");
            bannerMap.put("T18","51");
            bannerMap.put("A11-A14","52");
            bannerMap.put("B7","53");
            bannerMap.put("SZ08","54");
            bannerMap.put("3","55");
            bannerMap.put("T22","56");
            bannerMap.put("6-8","57");
            bannerMap.put("A14","58");
            bannerMap.put("C-D","59");
            bannerMap.put("26","60");
            bannerMap.put("T19","61");
            bannerMap.put("6","62");
            bannerMap.put("D","63");
            bannerMap.put("21","64");
            bannerMap.put("09","65");
            bannerMap.put("23","66");
            bannerMap.put("2-3","67");
            bannerMap.put("11","68");
            bannerMap.put("中缝","69");
            bannerMap.put("A04","70");
            bannerMap.put("A13-15","71");
            bannerMap.put("A06-A07","72");
            bannerMap.put("T3","73");
            bannerMap.put("A22","74");
            bannerMap.put("AI01","75");
            bannerMap.put("艺8","76");
            bannerMap.put("4","77");
            bannerMap.put("T17","78");
            bannerMap.put("T6","79");
            bannerMap.put("A08","80");
            bannerMap.put("A14-15","81");
            bannerMap.put("A8","82");
            bannerMap.put("SZ02","83");
            bannerMap.put("B2","84");
            bannerMap.put("49","85");
            bannerMap.put("A18","86");
            bannerMap.put("AII02","87");
            bannerMap.put("B4","88");
            bannerMap.put("08-09","89");
            bannerMap.put("16","90");
            bannerMap.put("SZ04","91");
            bannerMap.put("A7-A9","92");
            bannerMap.put("A07","93");
            bannerMap.put("06","94");
            bannerMap.put("A13","95");
            bannerMap.put("08","96");
            bannerMap.put("01","97");
            bannerMap.put("22","98");
            bannerMap.put("T2","99");
            bannerMap.put("31","100");
            bannerMap.put("T4","101");
            bannerMap.put("T9","102");
            bannerMap.put("T08","103");
            bannerMap.put("10","104");
            bannerMap.put("14-15","105");
            bannerMap.put("27","106");
            bannerMap.put("A10-A12","107");
            bannerMap.put("5-7","108");
            bannerMap.put("YZ13","109");
            bannerMap.put("A15","110");
            bannerMap.put("13","111");
            bannerMap.put("B6-B8","112");
            bannerMap.put("T7","113");
            bannerMap.put("艺1","114");
            bannerMap.put("T13","115");
            bannerMap.put("A05-A06","116");
            bannerMap.put("24","117");
            bannerMap.put("T27","118");
            bannerMap.put("29","119");
            bannerMap.put("A4-A9","120");
            bannerMap.put("14","121");
            bannerMap.put("19","122");
            bannerMap.put("A07-A08","123");
            bannerMap.put("A05","124");
            bannerMap.put("B5","125");
            bannerMap.put("20","126");
            bannerMap.put("A03","127");
            bannerMap.put("12","128");
            bannerMap.put("A17","129");
            bannerMap.put("A5","130");
            bannerMap.put("A14-A15","131");
            bannerMap.put("17","132");
            bannerMap.put("7","133");
            bannerMap.put("A12-A13","134");
            bannerMap.put("11-12","135");
            bannerMap.put("A11","136");
            bannerMap.put("07","137");
            bannerMap.put("A8-A9","138");
            bannerMap.put("30","139");
            bannerMap.put("C","140");
            bannerMap.put("03","141");
            bannerMap.put("B1","142");
            bannerMap.put("A7-A10","143");
            bannerMap.put("B3-B4","144");
            bannerMap.put("A1","145");
            bannerMap.put("10-11","146");
            bannerMap.put("T30","147");
            bannerMap.put("15-16","148");
            bannerMap.put("8","149");
            bannerMap.put("6-7","150");
            bannerMap.put("25","151");
            bannerMap.put("YC11","152");
            bannerMap.put("SZ07","153");
            bannerMap.put("B11","154");
            bannerMap.put("YZ12","155");
            bannerMap.put("28","156");
            bannerMap.put("04","157");
            bannerMap.put("T1-T4","158");
            bannerMap.put("A9-A10","159");
            bannerMap.put("5-8","160");
            bannerMap.put("02","161");
            bannerMap.put("A24","162");
            bannerMap.put("A12","163");
            bannerMap.put("T23","164");
            bannerMap.put("T4-T5","165");
            bannerMap.put("48","166");
            bannerMap.put("5","167");
            bannerMap.put("艺7","168");
            bannerMap.put("1","169");
            bannerMap.put("T8","170");
            bannerMap.put("A06-A08","171");
            bannerMap.put("A18-A20","172");
            bannerMap.put("A6-A7","173");
            bannerMap.put("艺6","174");
            bannerMap.put("T5","175");
            bannerMap.put("B6-B16","176");
            bannerMap.put("36","177");
            bannerMap.put("B","178");
            bannerMap.put("9","179");
            bannerMap.put("A02","180");
            bannerMap.put("A6","181");
            bannerMap.put("A10","182");
            bannerMap.put("T1-T16","183");
            bannerMap.put("T10","184");
            bannerMap.put("A01","185");
            bannerMap.put("T12-13","186");
            bannerMap.put("SZ03","187");
            bannerMap.put("06-08","188");
            bannerMap.put("T11","189");
            bannerMap.put("B9","190");
            bannerMap.put("T14","191");
            bannerMap.put("A10-A11","192");
            bannerMap.put("18","193");
            return bannerMap.get(Str(banner));
        }
    }

    private static String getAdLevel(Object level) {
        if ("0".equals(level)) {
            return "0";
        } else {
            return "1";
        }
    }

    private static String getAdvType(Object type) {
        if ("电视".equals(type)) {
            return "1";
        } else if ("广播".equals(type)) {
            return "2";
        } else if ("报纸".equals(type)) {
            return "3";
        } else return "2";
    }

    private static String getMeidaCode(Object meida) {
        Map<String, String> mediaMap = new HashMap<>();
        mediaMap.put("南京少儿频道","6");
        mediaMap.put("南京新闻综合频道","7");
        mediaMap.put("南京影视频道","8");
        mediaMap.put("南京生活频道","9");
        mediaMap.put("南京娱乐频道","10");
        mediaMap.put("南京信息频道","11");
        mediaMap.put("南京教科频道","12");
        mediaMap.put("南京十八频道","13");
        mediaMap.put("江苏城市频道","15");
        mediaMap.put("江苏综艺频道","16");
        mediaMap.put("江苏影视频道","17");
        mediaMap.put("江苏教育频道","18");
        mediaMap.put("江苏公共频道","19");
        mediaMap.put("南京新闻广播","27");
        mediaMap.put("南京交通广播","28");
        mediaMap.put("南京MAX广播(六合广播）","47");
        mediaMap.put("南京体育广播","3");
        mediaMap.put("南京城市管理广播","30");
        mediaMap.put("金陵晚报","48");
        mediaMap.put("东方卫报","70");
        mediaMap.put("江苏商报","51");
        mediaMap.put("南京日报","50");
        mediaMap.put("现代家庭报","52");
        mediaMap.put("周末","53");
        mediaMap.put("今日商报","71");
        mediaMap.put("南京广播电视报","69");
        mediaMap.put("现代快报","54");
        mediaMap.put("南京晨报","55");
        mediaMap.put("扬子晚报","56");
        mediaMap.put("新华日报","57");
        mediaMap.put("江苏科技报","59");
        mediaMap.put("江苏工人报","61");
        mediaMap.put("江苏经济报","58");
        mediaMap.put("江苏法治报","60");
        mediaMap.put("江南时报","66");
        mediaMap.put("南京经济广播","31");
        mediaMap.put("南京音乐广播FM105.8","32");
        mediaMap.put("江苏体育休闲频道","20");
        mediaMap.put("江苏经典流行音乐","33");
        mediaMap.put("江苏新闻综合广播","34");
        mediaMap.put("江苏健康广播","35");
        mediaMap.put("江苏文艺广播","36");
        mediaMap.put("江苏故事广播","37");
        mediaMap.put("江苏交通广播","42");
        mediaMap.put("金陵之声","38");
        mediaMap.put("江苏财经广播","39");
        mediaMap.put("江宁新闻综合频道","22");
        mediaMap.put("江宁生活资讯频道","23");
        mediaMap.put("溧水影视娱乐频道","25");
        mediaMap.put("溧水新闻综合频道","24");
        mediaMap.put("高淳新闻频道","26");
        mediaMap.put("高淳生活频道","");
        mediaMap.put("大众证券报","49");
        mediaMap.put("江苏音乐广播","41");
        mediaMap.put("江苏好享购物","21");
        mediaMap.put("江苏国际频道","72");
        mediaMap.put("江苏优漫卡通","68");
        mediaMap.put("江苏卫视","14");
        mediaMap.put("江苏新闻广播","40");
        mediaMap.put("浦口广播","46");
        mediaMap.put("江宁广播","43");
        mediaMap.put("溧水广播","44");
        mediaMap.put("高淳广播","45");
        mediaMap.put("江苏农业科技报","65");
        mediaMap.put("江苏广播电视报","67");
        mediaMap.put("老年周报","63");
        mediaMap.put("江苏教育报","62");
        mediaMap.put("凤凰资讯报","64");
        return mediaMap.get(Str(meida));
    }

    private static String getCateGoryId(Object category) {
        Map<String, String> categoryMap = new HashMap<>();
        categoryMap.put("药品类","1");
        categoryMap.put("医疗器械类","2");
        categoryMap.put("化妆品类","3");
        categoryMap.put("房地产类","4");
        categoryMap.put("普通食品类","5");
        categoryMap.put("特殊食品","6");
        categoryMap.put("烟草类","7");
        categoryMap.put("酒类","8");
        categoryMap.put("电器类","9");
        categoryMap.put("交通产品类","10");
        categoryMap.put("知识产品类","11");
        categoryMap.put("普通商品类","12");
        categoryMap.put("医疗服务类","13");
        categoryMap.put("金融服务类","14");
        categoryMap.put("信息服务类","15");
        categoryMap.put("互联网服务类","16");
        categoryMap.put("商业招商投资","17");
        categoryMap.put("教育培训服务类","18");
        categoryMap.put("会展文化活动服务类","19");
        categoryMap.put("普通服务类","20");
        categoryMap.put("形象宣传类","21");
        categoryMap.put("非商业类","22");
        categoryMap.put("其它类","23");
        categoryMap.put("处方药","101");
        categoryMap.put("非处方药","102");
        categoryMap.put("医疗器械","103");
        categoryMap.put("普通化妆品","104");
        categoryMap.put("特殊化妆品","105");
        categoryMap.put("商品房","106");
        categoryMap.put("商用地产／非民用","107");
        categoryMap.put("其他房地产","108");
        categoryMap.put("乳制品","109");
        categoryMap.put("饮料","110");
        categoryMap.put("奶粉","111");
        categoryMap.put("冲饮食品","112");
        categoryMap.put("方便食品","113");
        categoryMap.put("焙烤食品","114");
        categoryMap.put("食用油脂","115");
        categoryMap.put("米面杂粮","116");
        categoryMap.put("调味品","117");
        categoryMap.put("其他普通食品","118");
        categoryMap.put("保健食品","119");
        categoryMap.put("婴幼儿配方乳品","211");
        categoryMap.put("特殊医学用途配方食品","212");
        categoryMap.put("烟草制品","120");
        categoryMap.put("其他烟草","121");
        categoryMap.put("酒","122");
        categoryMap.put("视听设备","123");
        categoryMap.put("空气调节设备","124");
        categoryMap.put("通信设备","125");
        categoryMap.put("厨卫电器","126");
        categoryMap.put("照摄像产品","127");
        categoryMap.put("小家电产品","128");
        categoryMap.put("其他电器","129");
        categoryMap.put("机动车","130");
        categoryMap.put("非机动车","131");
        categoryMap.put("汽车用品","132");
        categoryMap.put("其他交通产品","133");
        categoryMap.put("软件","134");
        categoryMap.put("音像印刷出版物","135");
        categoryMap.put("其他知识产品","136");
        categoryMap.put("洁齿用品","139");
        categoryMap.put("卫生用品","140");
        categoryMap.put("清洁用品","141");
        categoryMap.put("计算机及配套设备","142");
        categoryMap.put("服饰箱包","143");
        categoryMap.put("家具家私","144");
        categoryMap.put("纺织品","145");
        categoryMap.put("办公学习用品","146");
        categoryMap.put("首饰","147");
        categoryMap.put("钟表眼镜","148");
        categoryMap.put("收藏品","149");
        categoryMap.put("厨房用具","150");
        categoryMap.put("建材五金","151");
        categoryMap.put("文体娱乐产品","152");
        categoryMap.put("农林渔牧物资","154");
        categoryMap.put("工业设备","155");
        categoryMap.put("宠物产品","156");
        categoryMap.put("其他普通商品","157");
        categoryMap.put("成人用品","213");
        categoryMap.put("医疗诊疗服务","158");
        categoryMap.put("医疗美容服务","159");
        categoryMap.put("投资理财","214");
        categoryMap.put("借贷","215");
        categoryMap.put("金融中介服务","216");
        categoryMap.put("其他金融服务","168");
        categoryMap.put("普通电话服务","169");
        categoryMap.put("移动通信服务","170");
        categoryMap.put("声讯服务","171");
        categoryMap.put("其他信息服务","172");
        categoryMap.put("电子商务","173");
        categoryMap.put("其它互联网服务","174");
        categoryMap.put("加工揽承","175");
        categoryMap.put("招商","176");
        categoryMap.put("特许加盟","177");
        categoryMap.put("其他商业招商投资","178");
        categoryMap.put("教育","179");
        categoryMap.put("培训","180");
        categoryMap.put("其他教育培训服务","181");
        categoryMap.put("会展","182");
        categoryMap.put("文体活动","183");
        categoryMap.put("其他会展文体活动服务","184");
        categoryMap.put("销售服务","185");
        categoryMap.put("职业中介","186");
        categoryMap.put("旅游服务","187");
        categoryMap.put("票务服务","188");
        categoryMap.put("餐饮服务","189");
        categoryMap.put("宾馆服务","190");
        categoryMap.put("运输服务","191");
        categoryMap.put("休闲娱乐服务","192");
        categoryMap.put("美容美发美体（除医疗美容）","193");
        categoryMap.put("装饰装潢服务","194");
        categoryMap.put("婚介婚庆服务","195");
        categoryMap.put("出入境中介服务","196");
        categoryMap.put("企业登记代理","197");
        categoryMap.put("法律服务","198");
        categoryMap.put("邮政快递服务","199");
        categoryMap.put("摄影服务","200");
        categoryMap.put("咨询服务","201");
        categoryMap.put("修理和维护服务","202");
        categoryMap.put("房产中介服务","203");
        categoryMap.put("其他普通服务","204");
        categoryMap.put("形象宣传","205");
        categoryMap.put("公告启示","206");
        categoryMap.put("公益广告","207");
        categoryMap.put("企业招聘","208");
        categoryMap.put("其他非商业","209");
        categoryMap.put("其它","210");
        categoryMap.put("保健用品","137");
        categoryMap.put("消毒产品","138");
        categoryMap.put("农兽药","153");
        return categoryMap.get(Str(category));
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

    private static Connection getSqlServerConnection() throws SQLException, ClassNotFoundException {
        // MySQL连接信息
        String url = "jdbc:sqlserver://localhost:1433;databaseName=adverNJ2021";
        String user = "sa";
        String password = "y@cp3winer";
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}
