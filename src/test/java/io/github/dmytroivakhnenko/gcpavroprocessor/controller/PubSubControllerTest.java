package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobInfo;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.PubSubEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = PubSubController.class)
public class PubSubControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private GCSFileProcessorService gcsFileProcessorService;

    @Test
    void whenEventMessageIsNullReturnBadRequestStatus() throws Exception {
        var event = new PubSubEvent();
        event.setMessage(null);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isBadRequest());

        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        verify(gcsFileProcessorService, times(0)).processFileViaIntegration(blobInfoCaptor.capture());
    }

    @Test
    void whenEventMessageFileNameIsNullReturnBadRequestStatus() throws Exception {
        var event = new PubSubEvent();
        var message = event.new Message();
        var data = new String(Base64.getEncoder().encode("{ \"bucket\": \"mybucket\", \"name\": null }".getBytes()));
        message.setData(data);
        event.setMessage(message);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isBadRequest());

        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        verify(gcsFileProcessorService, times(0)).processFileViaIntegration(blobInfoCaptor.capture());
    }

    @Test
    void whenEventMessageBucketNameIsNullReturnBadRequestStatus() throws Exception {
        var event = new PubSubEvent();
        var message = event.new Message();
        var data = new String(Base64.getEncoder().encode("{ \"bucket\": null, \"name\": \"myname\" }".getBytes()));
        message.setData(data);
        event.setMessage(message);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isBadRequest());

        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        verify(gcsFileProcessorService, times(0)).processFileViaIntegration(blobInfoCaptor.capture());
    }

    @Test
    void whenEventMessageIsValidThenProcessFile() throws Exception {
        var event = new PubSubEvent();
        var message = event.new Message();
        var data = new String(Base64.getEncoder().encode("{ \"bucket\": \"mybucket\", \"name\": \"myname\" }".getBytes()));

        message.setData(data);
        event.setMessage(message);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk());

        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        verify(gcsFileProcessorService, times(1)).processFileViaIntegration(blobInfoCaptor.capture());
        assertThat(blobInfoCaptor.getValue().getBucket()).isEqualTo("mybucket");
        assertThat(blobInfoCaptor.getValue().getName()).isEqualTo("myname");
    }
}
