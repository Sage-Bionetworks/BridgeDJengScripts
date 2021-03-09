package org.sagebionetworks.bridge.helper;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class LogHelper {
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");

    public static void logInfo(String msg) {
        System.out.print('[');
        System.out.print(DateTime.now(LOCAL_TIME_ZONE));
        System.out.print(']');
        System.out.println(msg);
    }

    public static void logError(String msg) {
        System.err.print('[');
        System.err.print(DateTime.now(LOCAL_TIME_ZONE));
        System.err.print(']');
        System.err.println(msg);
    }

    public static void logError(String msg, Throwable ex) {
        System.err.print('[');
        System.err.print(DateTime.now(LOCAL_TIME_ZONE));
        System.err.print(']');
        System.err.println(msg);
        ex.printStackTrace();
    }
}
