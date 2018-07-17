package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Date;
import java.util.ArrayList;
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

    /**
     * Replaces instances of ? in the sql string at the given locations with the given params.
     * <p>
     * We use JDBC 1-indexing.
     *
     * @param sql    SQL string
     * @param locs   instances of ? to be replaced
     * @param params replacement values
     * @return new SQL string with substitutions made
     */
    public static String replaceParams(String sql, int[] locs, String[] params) {
        ArrayList<Integer> splits = new ArrayList<>();
        {
            String tmp = sql;
            int index = tmp.indexOf("?");
            int off = 0;
            while (index != -1) {
                splits.add(index + off);
                tmp = tmp.substring(index + 1);
                off += index + 1;
                index = tmp.indexOf("?");
            }
        }

        String result = sql;
        {
            int off = 0;
            for (int i = 0; i < locs.length; i++) {
                int paramLoc = locs[i] - 1; // match JDBC
                int cut = off + splits.get(paramLoc);
                String pre = result.substring(0, cut);
                String post;
                if (cut == result.length()) {
                    post = "";
                } else {
                    post = result.substring(cut + 1);
                }
                result = String.format("%s%s%s", pre, params[i], post);
                off += params[i].length() - 1;
            }
        }

        return result;
    }

}
