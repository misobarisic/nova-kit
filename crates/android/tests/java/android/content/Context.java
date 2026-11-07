package android.content;

/** The few Context members nova-android calls, so a host JVM can stand in for Android. */
public abstract class Context {
    public Context getApplicationContext() {
        return this;
    }

    public ClassLoader getClassLoader() {
        return Context.class.getClassLoader();
    }

    public java.io.File getFilesDir() {
        return new java.io.File(System.getProperty("fixture.files", "/tmp/fixture-files"));
    }
}
