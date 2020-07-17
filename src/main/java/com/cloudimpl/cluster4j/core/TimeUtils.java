package com.cloudimpl.cluster4j.core;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

public class TimeUtils {

  private static long offset = 0;
  private static DateTimeZone zone = DateTimeZone.forID("EST5EDT");

  public static long getOffset() {
    return offset;
  }

  public boolean timeCheck(long transactTime) {
    // long curTime = TimeUtils.currentTimeMillis();
    // long minute = 60000;
    // return transactTime >= (curTime - minute);
    return TimeUtils.getCurrentTime().minusMinutes(1).isAfter(transactTime);
  }

  public static long getDayFromMillis(long time) {
    return TimeUtils.fromEpoch(time).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
        .withMillisOfSecond(0).getMillis();
  }

  public static void setOffset(long offset) {
    TimeUtils.offset = offset;
  }

  public static void setZone(DateTimeZone zone) {
    TimeUtils.zone = zone;
  }

  public static long currentTimeMillis() {
    return (offset == 0) ? System.currentTimeMillis() : TimeUtils.getCurrentTime().getMillis();
  }

  public static DateTime getCurrentTime() {
    return (offset == 0) ? DateTime.now().withZone(zone) : DateTime.now().plus(offset).withZone(zone);
  }

  public static DateTime fromEpoch(long mills) {
    return new DateTime(mills).withZone(zone);
  }

  public static DateTime fromString(String string, String format) {
    DateTime dt = DateTime.parse(string, DateTimeFormat.forPattern(format).withZone(zone));
    if (dt.getYear() != 1970) {
      return dt;
    } else {
      DateTime currentDate = TimeUtils.getCurrentTime();
      return dt.withYear(currentDate.getYear()).withMonthOfYear(currentDate.getMonthOfYear())
          .withDayOfMonth(currentDate.getDayOfMonth()).withMillisOfDay(0);
    }
  }

  public static String toStringDateOnly(DateTime date) {
    return date.toString("MM/dd/yyyy");
  }

  public static String toStringTimeOnly(DateTime date) {
    return date.toString("h:mm a");
  }

  public static String toStringDateTime(DateTime date) {
    return date.toString("MM/dd/yyyy h:mm a");
  }

  public static DateTimeZone getTimezone() {
    return zone;
  }

  public static String millsToString(long mills) {
    long ms = mills % 1000;
    long msx = mills / 1000;
    long s = msx % 60;
    long sx = msx / 60;
    long m = sx % 60;
    long h = sx / 60;
    return String.format("%02d:%02d:%02d.%03d", h, m, s, ms);
  }

  public static String toStringDateTimeWithTimezone(DateTime date, String format) {
    return date.toString(format) + " EST";
  }
}
