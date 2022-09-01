package com.hwj.util;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String dateFormat(LocalDateTime date) {

        return date.format(dateFormat);
    }

    public static String dateFormat(LocalDate date) {

        return date.format(dateFormat);
    }

    public static String dateFormat(Date date) {

        return date.toInstant().atOffset(ZoneOffset.of("+8")).toLocalDateTime().format(dateFormat);
    }

    public static String formatCustomized(LocalDateTime date, DateTimeFormatter formatter) {

        return date.format(formatter);
    }

    public static String formatCustomized(Date date, DateTimeFormatter formatter) {

        return date.toInstant().atOffset(ZoneOffset.of("+8")).toLocalDateTime().format(formatter);
    }

    public static Date dateParse(String str) {

        Instant instant = LocalDate.parse(str, dateFormat).atStartOfDay(ZoneOffset.of("+8")).toInstant();
        return Date.from(instant);
    }

    public static Date dateParse(String str,DateTimeFormatter dateFormat) {

        Instant instant = LocalDate.parse(str, dateFormat).atStartOfDay(ZoneOffset.of("+8")).toInstant();
        return Date.from(instant);
    }

    public static LocalDate localDateParse(String str) {

        return LocalDate.parse(str, dateFormat);
    }

    public static LocalTime localTimeParse(String str) {

        return LocalTime.parse(str, timeFormat);
    }

    public static String dateTimeFormat(LocalDateTime date) {

        return date.format(dateTimeFormat);
    }

    public static String dateTimeFormat(Date date) {

        return date.toInstant().atOffset(ZoneOffset.of("+8")).toLocalDateTime().format(dateTimeFormat);
    }

    public static Date dateTimeParse(String str) {

        return new Date(Timestamp.valueOf(LocalDateTime.parse(str, dateTimeFormat)).getTime());
    }

    public static LocalDateTime localDateTimeParse(String str) {

        return LocalDateTime.parse(str, dateTimeFormat);
    }

    public static Timestamp getFtime(long time) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(time));
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        int curMinute = calendar.get(Calendar.MINUTE);
        calendar.set(Calendar.MINUTE, curMinute % 5 == 0 ? curMinute - 5 : curMinute - curMinute % 5);
        return new Timestamp(calendar.getTime().getTime());
    }

    public static Timestamp getTtime(long time) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(time));
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        int curMinute = calendar.get(Calendar.MINUTE);
        calendar.set(Calendar.MINUTE, curMinute % 5 == 0 ? curMinute : curMinute - curMinute % 5 + 5);
        return new Timestamp(calendar.getTime().getTime());
    }

    public static long getMorning0() {

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    public static long getNextMorning0() {

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    public static Date getYesterdayMorning0() {

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static Date addDay(Date d, int amount) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.DAY_OF_MONTH, amount);
        return calendar.getTime();
    }


    public static void main(String[] args) {

        String[] split = "abd".split(";");
        System.out.println(split[0]);

        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(1584675752000l, 0, ZoneOffset.ofHours(8));
        System.out.println(dateTimeFormat(localDateTime));
    }
}
