package beian;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/3/20 15:55
 */
public class Test {
    public static void main(String[] args) throws ParseException, JsonProcessingException {

//        String viewLog = HuwaiBeianData2.getViewLog("2017-02-15 15:13:27  创建广告,存入草稿</br>2017-02-15 15:39:21  修改广告并提交平台查阅</br>2017-02-16 09:48:14  工商查阅被退回,查阅人:徐向威 退回原因:提供商标证和发布地点的设置许可、发布单位的营业执照。。</br>2017-02-19 16:45:26  修改广告并提交平台查阅</br>2017-02-20 14:59:49  工商查阅被退回,查阅人:徐向威 退回原因:提供商标证和发布地点的设置许可。</br>2017-02-21 16:33:25  修改广告并提交平台查阅</br>2017-02-22 10:55:49  工商查阅被退回,查阅人:徐向威 退回原因:商标证过期，提供有效的。</br>2017-02-23 10:12:13  修改广告并提交平台查阅</br>2017-02-23 10:38:36  工商查阅通过,查阅人:徐向威");
//        System.out.println(viewLog);
        String s = "lr7br3t_nDdjaw692zpA3VNSDESP.mp4";
        String[] split = s.split(",");
        for (String s1 : split) {
            System.out.println(s1);
        }
    }

    private static String getPlayTime(String timeRanges) throws ParseException {
        String[] ranges = timeRanges.split(",");

        Date min = null;
        Date max = null;

        SimpleDateFormat format = new SimpleDateFormat("HH:mm");

        for(String range : ranges) {
            String[] times = range.split("-");
            Date start = format.parse(times[0]);
            Date end = format.parse(times[1]);

            if(min == null || start.before(min)) {
                min = start;
            }

            if(max == null || end.after(max)) {
                max = end;
            }
        }

        return format.format(min) + "--" + format.format(max);
    }
}
