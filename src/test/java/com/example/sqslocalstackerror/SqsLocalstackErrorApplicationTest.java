package com.example.sqslocalstackerror;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@SpringBootTest
@Testcontainers
class SqsLocalstackErrorApplicationTest {

    public static LocalStackContainer localstackContainer;

    static {
        localstackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3"))
                .withServices(LocalStackContainer.Service.SQS);
        localstackContainer.start();
    }

    @DynamicPropertySource
    @SneakyThrows
    static void registerProperties(DynamicPropertyRegistry registry) {
        execInContainer(localstackContainer, "awslocal", "sqs", "create-queue", "--queue-name", "prova.fifo", "--attributes", "FifoQueue=true,ContentBasedDeduplication=false");

        registry.add("spring.cloud.aws.credentials.access-key", localstackContainer::getAccessKey);
        registry.add("spring.cloud.aws.credentials.secret-key", localstackContainer::getSecretKey);
        registry.add("spring.cloud.aws.region.static", localstackContainer::getRegion);
        registry.add("spring.cloud.aws.sqs.endpoint", () -> localstackContainer.getEndpointOverride(LocalStackContainer.Service.SQS));
    }

    @SneakyThrows
    private static void execInContainer(ContainerState container, String... parameters) {
        Container.ExecResult execResult = container.execInContainer(parameters);
        Assertions.assertThat(execResult.getExitCode()).isZero();
    }

    @Autowired
    private SqsAsyncClient sqsClient;


    @Test
    @DisplayName("Should receive all messages because deduplication id is different")
    @SneakyThrows
    void scenario01() {
        var queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName("prova.fifo").build()).get().queueUrl();

        for (int i = 0; i < 50; i++) {
            var sendMessageResponse = sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("{}")
                    .messageGroupId("group01")
                    .messageDeduplicationId("differentDeduplicationId" + i)
                    .build()).get();
            Assertions.assertThat(sendMessageResponse.messageId()).isNotBlank();
            Assertions.assertThat(sendMessageResponse.sdkHttpResponse().isSuccessful()).isTrue();

            var receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build()).get();
            Assertions.assertThat(receiveMessageResponse.messages()).hasSize(1);
        }
    }

    @Test
    @DisplayName("should receive only one message because deduplication id is always the same")
    @SneakyThrows
    void scenario02() {
        var queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName("prova.fifo").build()).get().queueUrl();

        for (int i = 0; i < 50; i++) {
            var sendMessageResponse = sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("{}")
                    .messageGroupId("group02")
                    .messageDeduplicationId("sameDeduplicationId")
                    .build()).get();
            Assertions.assertThat(sendMessageResponse.messageId()).isNotBlank();
            Assertions.assertThat(sendMessageResponse.sdkHttpResponse().isSuccessful()).isTrue();

            var receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build()).get();
            if (i == 0) {
                Assertions.assertThat(receiveMessageResponse.messages()).hasSize(1);
            } else {
                Assertions.assertThat(receiveMessageResponse.messages()).isEmpty();
            }
        }
    }

    @Test
    @DisplayName("should receive only one message even though the content is different")
    @SneakyThrows
    void scenario03() {
        var queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName("prova.fifo").build()).get().queueUrl();

        for (int i = 0; i < 50; i++) {
            var sendMessageResponse = sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("{" + i + "}")
                    .messageGroupId("group03")
                    .messageDeduplicationId("sameDeduplicationId")
                    .build()).get();
            Assertions.assertThat(sendMessageResponse.messageId()).isNotBlank();
            Assertions.assertThat(sendMessageResponse.sdkHttpResponse().isSuccessful()).isTrue();

            var receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build()).get();
            if (i == 0) {
                Assertions.assertThat(receiveMessageResponse.messages()).hasSize(1);
            } else {
                Assertions.assertThat(receiveMessageResponse.messages()).isEmpty();
            }
        }
    }

    @Test
    @DisplayName("Should receive all messages because deduplication id is different - Home Made Sqs Client")
    @SneakyThrows
    void scenario01butManualClient() {
        var manualSqsClient = SqsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstackContainer.getAccessKey(), localstackContainer.getSecretKey()))
                )
                .endpointOverride(localstackContainer.getEndpointOverride(LocalStackContainer.Service.SQS))
                .region(Region.of(localstackContainer.getRegion()))
                .build();

        var queueUrl = manualSqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName("prova.fifo").build()).queueUrl();

        for (int i = 0; i < 50; i++) {
            var sendMessageResponse = manualSqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("{}")
                    .messageGroupId("group04")
                    .messageDeduplicationId("differentDeduplicationId" + i)
                    .build());
            Assertions.assertThat(sendMessageResponse.messageId()).isNotBlank();
            Assertions.assertThat(sendMessageResponse.sdkHttpResponse().isSuccessful()).isTrue();

            var receiveMessageResponse = manualSqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build());
            Assertions.assertThat(receiveMessageResponse.messages()).hasSize(1);
        }
    }
}
