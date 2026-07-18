package io.matrisk;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class EventConsumer {
    private static final JsonFormat CE_JSON = (JsonFormat) EventFormatProvider.getInstance()
            .resolveFormat(JsonFormat.CONTENT_TYPE);

    // match the type emitted by our workflow: "org.acme.email.review.required"
    private static final String EXECUTION_REQUEST_TYPE = "matrisk.processor.execution.request.v1";

    
    @Incoming("flow-out-incoming")  // Listen to a Kafka channel
    public void onFlowOut(byte[] record) {
        try {
            // Deserialize byte array to CloudEvent
            CloudEvent ce = CE_JSON.deserialize(record);
            
            if (ce == null || ce.getType() == null)
                return;

            if (EXECUTION_REQUEST_TYPE.equals(ce.getType())) {
            // Process the CloudEvent
            byte[] data = ce.getData() != null ? ce.getData().toBytes() : null;
            
            // If there's no data, send a minimal envelope so the UI can handle it.
            String json = (data == null || data.length == 0)
                    ? "{\"type\":\"" + EXECUTION_REQUEST_TYPE + "\",\"payload\":null}"
                    : new String(data, StandardCharsets.UTF_8);

            String workflowInstanceId = ce.getExtension("flowinstanceid").toString();
            
            // Handle event...
            
            Log.infof("Received execution request (workflow instance: %s) required event: %s",
                    workflowInstanceId, json);
            }
        } catch (Exception ex) {
            Log.error("Failed to consume event", ex);
        }
    }
}
