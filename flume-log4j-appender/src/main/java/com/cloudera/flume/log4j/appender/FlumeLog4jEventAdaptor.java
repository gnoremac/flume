package com.cloudera.flume.log4j.appender;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.core.EventBaseImpl;
import com.cloudera.util.NetUtils;

/**
 * This is an adaptor for converting the log4j logging interface into flumes.
 * 
 * This is assumed to only be present in the instrumented process, so getting
 * local host is reasonable for getting the host field.
 * 
 * This is not threadsafe.
 */

public class FlumeLog4jEventAdaptor extends EventBaseImpl {

    LoggingEvent evt;
    long nanos;

    protected final StringBuffer buf = new StringBuffer(256);
    private final Layout layout;

    public FlumeLog4jEventAdaptor(LoggingEvent evt, Layout layout) {
        super();
        this.evt = evt;
        this.layout = layout;
        // Differentiate between events at the same millisecond
        this.nanos = System.nanoTime();
    }

    @Override
    public byte[] getBody() {
        // Reset buf
        buf.setLength(0);
        buf.append(this.layout.format(evt));
        // buf.append(evt.getTimeStamp());
        // buf.append('[');
        // buf.append(evt.getThreadName());
        // buf.append("] ");
        // buf.append("- ");
        // buf.append(evt.getRenderedMessage());
        if (layout.ignoresThrowable()) {
            String[] s = evt.getThrowableStrRep();
            if (s != null) {
                int len = s.length;
                for (int i = 0; i < len; i++) {
                    buf.append(s[i]);
                }
            }
        }
        return buf.toString().getBytes();
    }

    @Override
    public String getHost() {
        return NetUtils.localhost();
    }

    @Override
    public long getNanos() {
        return nanos;
    }

    @Override
    public Priority getPriority() {
        Level l = evt.getLevel();
        if (l == Level.DEBUG)
            return Priority.DEBUG;
        if (l == Level.INFO)
            return Priority.INFO;
        if (l == Level.WARN)
            return Priority.WARN;
        if (l == Level.ERROR)
            return Priority.ERROR;
        if (l == Level.FATAL)
            return Priority.FATAL;

        // default to info level
        return Priority.INFO;
    }

    @Override
    public long getTimestamp() {
        return evt.getTimeStamp();
    }

}
