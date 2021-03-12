package com.weshare.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.Chronology;
import java.time.chrono.JapaneseDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * @author ximing.wei
 */
public class DateUtil {

    private enum DateTypeEnum {
        DATE_FORMAT_0("\\d{4}(\\.)\\d{1,2}(\\.)\\d{1,2}", "yyyy.MM.dd"),
        DATE_FORMAT_1("\\d{4}(-)\\d{1,2}(-)\\d{1,2}", "yyyy-MM-dd"),
        DATE_FORMAT_2("\\d{4}(/)\\d{1,2}(/)\\d{1,2}", "yyyy/MM/dd"),
        DATE_FORMAT_3("\\d{13}", "ms"),
        DATE_FORMAT_4("\\d{10}", "s"),;

        private String pattern;
        private String dateType;

        DateTypeEnum(String pattern, String dateType) {
            this.pattern = pattern;
            this.dateType = dateType;
        }

        private String getPattern() {
            return pattern;
        }

        private String getDateType() {
            return dateType;
        }

        private static boolean getDateMatch(String pattern, CharSequence input) {
            return Pattern.compile(pattern).matcher(input).matches();
        }

        public static Date getDate(String dateString) throws ParseException {
            if (EmptyUtil.isEmpty(dateString)) return null;

            if (getDateMatch(DATE_FORMAT_0.getPattern(), dateString)) return dateFromType(dateString, DATE_FORMAT_0.getDateType());
            if (getDateMatch(DATE_FORMAT_1.getPattern(), dateString)) return dateFromType(dateString, DATE_FORMAT_1.getDateType());
            if (getDateMatch(DATE_FORMAT_2.getPattern(), dateString)) return dateFromType(dateString, DATE_FORMAT_2.getDateType());
            if (getDateMatch(DATE_FORMAT_3.getPattern(), dateString)) return dateFromType(dateString, DATE_FORMAT_3.getDateType());
            if (getDateMatch(DATE_FORMAT_4.getPattern(), dateString)) return dateFromType(dateString, DATE_FORMAT_4.getDateType());
            return dateFromType(dateString, DATE_FORMAT_0.getDateType());
        }
    }

    private static Date dateFromType(String dateString, String fromFmt) throws ParseException {
        return new SimpleDateFormat(fromFmt).parse(dateString);
    }

    public static String getDate(String string, String fromFmt, String toFmt) throws ParseException {
        if (EmptyUtil.isEmpty(string)) return null;
        Date date;
        switch (fromFmt) {
            case "ms":
                if (13 != string.length() && 10 != string.length()) return string;
                date = new Date(Long.valueOf(string.length() == 13 ? string : string + "000"));
                break;
            case "s":
                if (10 != string.length()) return string;
                date = new Date(Long.valueOf(string + "000"));
                break;
            case "":
                date = DateTypeEnum.getDate(string);
                break;
            default:
                if (string.length() != fromFmt.length()) return string;
                date = dateFromType(string, fromFmt);
                break;
        }
        return new SimpleDateFormat(toFmt).format(date);
    }

    public static void main(String[] args) {
        // java.time 包下 LocalDate  LocalTime  LocalDateTime  Instant  Duration  Period  ChronoLocalDate(日历)
        System.out.println(LocalDate.now()); // 2021-01-22
        System.out.println(LocalTime.now()); // 16:32:11.322
        System.out.println(LocalDate.now().atTime(LocalTime.now())); // 2021-01-22T16:32:11.322
        System.out.println(LocalTime.now().atDate(LocalDate.now())); // 2021-01-22T16:32:11.322
        System.out.println(LocalDateTime.now()); // 2021-01-22T16:32:11.322

        System.out.println(Instant.now()); // 2021-01-22T08:33:41.846Z 可以精确到纳秒的时间戳
        System.out.println(Instant.now().getEpochSecond() + "---"); // 秒级
        System.out.println(LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8")));

        // 可以到纳秒的时间间隔
        Duration duration = Duration.between(LocalDateTime.parse("2021-01-22T16:40:11.120"), LocalDateTime.parse("2021-01-22T16:32:11.322"));
        System.out.println(duration); // PT-8M0.202S
        System.out.println(duration.abs()); // PT7M59.798S
        System.out.println(duration.toDays()); // 0
        System.out.println(duration.toHours()); // 0
        System.out.println(duration.toMinutes()); // -8
        System.out.println(duration.getSeconds()); // -480
        System.out.println(duration.toMillis()); // -479798
        System.out.println(duration.toNanos()); // -479798000000

        // 只能到天级的时间间隔
        System.out.println(Period.between(LocalDate.parse("2021-01-22"), LocalDate.parse("2021-01-20"))); // P-2D

        // 日期在增加5年
        System.out.println(LocalDate.of(2021, 1, 1).plus(5, ChronoUnit.YEARS));

        System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        System.out.println(LocalDateTime.parse("2017-01-05 12:30:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        System.out.println(ZoneId.systemDefault());
        ZoneId shanghaiZoneId = ZoneId.of("Asia/Shanghai");
        System.out.println(shanghaiZoneId);
        LocalDateTime localDateTime = LocalDateTime.now();
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, shanghaiZoneId);
        System.out.println(zonedDateTime);

        ZoneOffset zoneOffset = ZoneOffset.of("+09:00");
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, zoneOffset);
        System.out.println(offsetDateTime);

        Chronology chronology = Chronology.ofLocale(Locale.JAPANESE);
        ChronoLocalDate chronoLocalDate = chronology.dateNow();
        System.out.println(chronoLocalDate);
        JapaneseDate japaneseDate = JapaneseDate.now();
        System.out.println(japaneseDate);

//        System.out.println();
//        for (String string : ZoneId.getAvailableZoneIds()) {
//            System.out.println(string);
//        }
    }
}
