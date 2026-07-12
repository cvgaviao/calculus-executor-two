package io.matrisk;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class EventConsumer {
    private static final JsonFormat CE_JSON = (JsonFormat) EventFormatProvider.getInstance()
            .resolveFormat(JsonFormat.CONTENT_TYPE);

    @Incoming("flow-out-incoming")  // Listen to a Kafka channel
    public void onFlowOut(byte[] record) {
        try {
            // Deserialize byte array to CloudEvent
            CloudEvent ce = CE_JSON.deserialize(record);
            
            if (ce == null || ce.getType() == null)
                return;

            // Process the CloudEvent
            String eventType = ce.getType();
            byte[] data = ce.getData() != null ? ce.getData().toBytes() : null;
            String workflowInstanceId = ce.getExtension("flowinstanceid").toString();
            
            // Handle event...
        } catch (Exception ex) {
            Log.error("Failed to consume event", ex);
        }
    }
}
