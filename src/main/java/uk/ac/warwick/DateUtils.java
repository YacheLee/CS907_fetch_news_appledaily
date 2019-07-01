package uk.ac.warwick;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
    public static Timestamp parseToTimeStamp(String str) throws ParseException {
        SimpleDateFormat simpleDateFormat = provideDateFormat();
        Date date = simpleDateFormat.parse(str);
        return new Timestamp(date.getTime());
    }

    static SimpleDateFormat provideDateFormat() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyymmdd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Taipei"));
        return simpleDateFormat;
    }

    public static String getStartDate(int year, int month){
        return year+"/"+month+"/"+1;
    }

    public static int getEndDateOfMonth(int year, int month){
        YearMonth yearMonthObject = YearMonth.of(year, month);
        return yearMonthObject.lengthOfMonth();
    }
}