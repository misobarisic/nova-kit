package dev.example;

import android.content.Context;

/** Stand-ins for an Activity, the Application and the download service. */
public final class Fixture {
    public static final class App extends Context {}

    public static final class Activity extends Context {
        private final App app;

        Activity(App app) {
            this.app = app;
        }

        @Override
        public Context getApplicationContext() {
            return app;
        }
    }

    private static App app;
    private static Context lastContext;
    private static int starts;
    private static int stops;
    private static int updates;
    private static String lastText = "";
    private static int rejectStarts;

    public static Activity bootstrapActivity() {
        app = new App();
        return new Activity(app);
    }

    public static boolean isApplication(Context context) {
        return context == app;
    }

    public static void rejectNextStarts(int count) {
        rejectStarts = count;
    }

    public static int starts() { return starts; }
    public static int stops() { return stops; }
    public static int updates() { return updates; }
    public static String lastText() { return lastText; }
    public static boolean calledWithApplication() { return lastContext == app; }

    /** The Java side of the download service: static start/stop/update. */
    public static final class Service {
        public static void start(Context context) {
            lastContext = context;
            if (rejectStarts > 0) {
                rejectStarts--;
                throw new SecurityException("foreground start not allowed");
            }
            starts++;
        }

        public static void stop(Context context) {
            lastContext = context;
            stops++;
        }

        public static void update(Context context, String title, String text) {
            lastContext = context;
            updates++;
            lastText = title + "/" + text;
        }
    }

    /** Throws, so a native caller leaves a pending exception behind. */
    public static void explode(Context context) {
        throw new IllegalStateException("boom");
    }
}
