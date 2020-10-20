package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;
import com.google.gson.GsonBuilder;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.PubSubEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
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

    @Mock
    private CompletableFuture completableFuture;

    @Mock
    private Job job;

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
        var data = getEncodedJsonData("mybucket", null);
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
        var data = getEncodedJsonData(null, "myname.avro");
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
        when(gcsFileProcessorService.processFileViaIntegration(Mockito.any(BlobInfo.class))).thenReturn(completableFuture);
        when(completableFuture.get(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(job);
        when(job.waitFor(Mockito.any(RetryOption.class))).thenReturn(job);
        var event = new PubSubEvent();
        var message = event.new Message();
        var data = getEncodedJsonData("mybucket", "myname.avro");

        message.setData(data);
        event.setMessage(message);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk());


        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);

        verify(gcsFileProcessorService, times(1)).processFileViaIntegration(blobInfoCaptor.capture());
        assertThat(blobInfoCaptor.getValue().getBucket()).isEqualTo("mybucket");
        assertThat(blobInfoCaptor.getValue().getName()).isEqualTo("myname.avro");
    }

    @Test
    void whenFileIsNotAvroFileSkipItAndReturnOk() throws Exception {
        var event = new PubSubEvent();
        var message = event.new Message();
        var data = getEncodedJsonData("mybucket", "name.txt");
        message.setData(data);
        event.setMessage(message);
        mockMvc.perform(post("/pubsub")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk());

        var blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        verify(gcsFileProcessorService, times(0)).processFileViaIntegration(blobInfoCaptor.capture());
    }

    private String getEncodedJsonData(final String bucket, final String name) {
        var myMap = new HashMap<String, String>();
        myMap.put("bucket", bucket);
        myMap.put("name", name);

        var gson = new GsonBuilder().create();
        return new String(Base64.getEncoder().encode(gson.toJson(myMap).getBytes()));
    }
}
