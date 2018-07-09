package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Date;
import java.util.Calendar;

public class TPCCLikeUtil {

    /**
     * Adds days to the given date.
     *
     * @param date      start date
     * @param daysToAdd number of days to add
     * @return new date
     */
    public static Date addDays(Date date, int daysToAdd) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysToAdd);
        return new Date(cal.getTimeInMillis());
    }

}
