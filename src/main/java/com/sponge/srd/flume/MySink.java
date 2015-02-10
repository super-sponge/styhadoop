package com.sponge.srd.flume;


import com.google.common.base.Strings;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link org.apache.flume.Sink} implementation that logs all events received at the INFO level
 * to the <tt>org.apache.flume.sink.LoggerSink</tt> logger.
 * </p>
 * <p>
 * <b>WARNING:</b> Logging events can potentially introduce performance
 * degradation.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <p>
 * <i>This sink has no configuration parameters.</i>
 * </p>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class MySink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MySink.class);
    private String myProp;
    // Default Max bytes to dump
    public static final int DEFAULT_MAX_BYTE_DUMP = 16;

    // Max number of bytes to be dumped
    private int maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;

    public static final String MAX_BYTES_DUMP_KEY = "maxBytesToLog";


    @Override
    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");

        // Process the myProp value (e.g. validation)

        // Store myProp for later retrieval by process() method
        if (!Strings.isNullOrEmpty(myProp)){
            this.myProp = myProp;
        } else {
            this.myProp = "default";
        }

        String strMaxBytes = context.getString(MAX_BYTES_DUMP_KEY);
        if (!Strings.isNullOrEmpty(strMaxBytes)) {
            try {
                maxBytesToLog = Integer.parseInt(strMaxBytes);
            } catch (NumberFormatException e) {
                logger.warn(String.format("Unable to convert %s to integer, using default value(%d) for maxByteToDump",
                        strMaxBytes, DEFAULT_MAX_BYTE_DUMP));
                maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;
            }
        }
    }

    @Override
    public void start() {
        // Initialize the connection to the external repository (e.g. HDFS) that
        // this Sink will forward Events to ..
    }

    @Override
    public void stop () {
        // Disconnect from the external respository and do any
        // additional cleanup (e.g. releasing resources or nulling-out
        // field values) ..
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();

            // Send the Event to the external repository.
            // storeSomeData(e);

            if (event != null) {
                if (logger.isInfoEnabled()) {
                    logger.info("Event: " + EventHelper.dumpEvent(event, maxBytesToLog));
                }
            } else {
                // No event found, request back-off semantics from the sink runner
                status = Status.BACKOFF;
            }

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }
}

