package com.polarbookshop.dispatcherservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class FunctionsStreamIntegrationTests {

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  private InputDestination input;

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  private OutputDestination output;

  @Autowired private ObjectMapper objectMapper;

  @Test
  void whenOrderAcceptedThenDispatched() throws IOException {
    long orderId = 121;
    Message<OrderAcceptedMessage> inputMessage =
        MessageBuilder.withPayload(new OrderAcceptedMessage(orderId)).build();
    Message<OrderDispatchedMessage> expectedOutputMessage =
        MessageBuilder.withPayload(new OrderDispatchedMessage(orderId)).build();

    input.send(inputMessage);

    assertThat(objectMapper.readValue(output.receive().getPayload(), OrderDispatchedMessage.class))
        .isEqualTo(expectedOutputMessage.getPayload());
  }
}
