/*
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.contribs.queue.amqp.config.AMQPEventQueueProperties;
import com.netflix.conductor.contribs.queue.amqp.util.AMQPSettings;

import com.rabbitmq.client.AMQP.PROTOCOL;
import com.rabbitmq.client.ConnectionFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AMQPSettingsTest {

    private AMQPEventQueueProperties properties;

    @BeforeEach
    void setUp() {
        properties = mock(AMQPEventQueueProperties.class);
        when(properties.getBatchSize()).thenReturn(1);
        when(properties.getPollTimeDuration()).thenReturn(Duration.ofMillis(100));
        when(properties.getHosts()).thenReturn(ConnectionFactory.DEFAULT_HOST);
        when(properties.getUsername()).thenReturn(ConnectionFactory.DEFAULT_USER);
        when(properties.getPassword()).thenReturn(ConnectionFactory.DEFAULT_PASS);
        when(properties.getVirtualHost()).thenReturn(ConnectionFactory.DEFAULT_VHOST);
        when(properties.getPort()).thenReturn(PROTOCOL.PORT);
        when(properties.getConnectionTimeoutInMilliSecs()).thenReturn(60000);
        when(properties.isUseNio()).thenReturn(false);
        when(properties.isDurable()).thenReturn(true);
        when(properties.isExclusive()).thenReturn(false);
        when(properties.isAutoDelete()).thenReturn(false);
        when(properties.getContentType()).thenReturn("application/json");
        when(properties.getContentEncoding()).thenReturn("UTF-8");
        when(properties.getExchangeType()).thenReturn("topic");
        when(properties.getDeliveryMode()).thenReturn(2);
        when(properties.isUseExchange()).thenReturn(true);
    }

    @Test
    void aMQPSettings_exchange_fromuri_defaultconfig() {
        String exchangestring =
                "amqp_exchange:myExchangeName?exchangeType=topic&routingKey=test&deliveryMode=2";
        AMQPSettings settings = new AMQPSettings(properties);
        settings.fromURI(exchangestring);
        assertEquals("topic", settings.getExchangeType());
        assertEquals("test", settings.getRoutingKey());
        assertEquals("myExchangeName", settings.getQueueOrExchangeName());
    }

    @Test
    void aMQPSettings_queue_fromuri_defaultconfig() {
        String exchangestring =
                "amqp_queue:myQueueName?deliveryMode=2&durable=false&autoDelete=true&exclusive=true";
        AMQPSettings settings = new AMQPSettings(properties);
        settings.fromURI(exchangestring);
        assertFalse(settings.isDurable());
        assertTrue(settings.isExclusive());
        assertTrue(settings.autoDelete());
        assertEquals(2, settings.getDeliveryMode());
        assertEquals("myQueueName", settings.getQueueOrExchangeName());
    }

    @Test
    void aMQPSettings_exchange_fromuri_wrongdeliverymode() {
        assertThrows(IllegalArgumentException.class, () -> {
            String exchangestring =
                            "amqp_exchange:myExchangeName?exchangeType=topic&routingKey=test&deliveryMode=3";
            AMQPSettings settings = new AMQPSettings(properties);
            settings.fromURI(exchangestring);
        });
    }
}
