/*
 * Copyright 2012-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.schedulingtasks;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

import javax.annotation.PostConstruct;

@Component
public class ScheduledTasks {
	private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);
	// Client
	private EventHubConsumerAsyncClient consumerClient;
	// Handler
	private Disposable subscription;
	// Connection info
	private final String ehConnectionStringFormat = "Endpoint=sb://%s.servicebus.windows.net/;SharedAccessKeyName=%s;SharedAccessKey=%s;EntityPath=%s";
	private final String namespaceName = "<namespace_name>";
	private final String keyName = "<sas_key_name>";
	private final String keyValue = "<sas_key>";
	private final String entityPath = "<eh_name>";
	private String ehConnectionString = String.format(ehConnectionStringFormat, namespaceName, keyName, keyValue, entityPath);
	private String consumerGroupName = "<consumer_group_name>";

	@PostConstruct
	private void postConstruct() {
		this.consumerClient = new EventHubClientBuilder()
				.connectionString(ehConnectionString)
				.consumerGroup(consumerGroupName)
				.prefetchCount(1)
				.buildAsyncConsumerClient();
	}

	@Scheduled(fixedDelay= 240000, initialDelay = 240000)
	public void subscribeEventsJob() {
		if(subscription == null || subscription.isDisposed()) {
			subscription = consumerClient.receiveFromPartition("0", EventPosition.latest())
					.subscribe(partitionEvent -> {
								try {
									log.info("...Got Event...");
									EventData event = partitionEvent.getData();
									//some other code here
								} catch (Exception e) {
									// in this case, this line is not executed
									log.error("exception-1:" + e.getMessage());
								}
							},
							error -> {
								// in this case, this line is not executed
								log.error("error-1:" + error.getMessage());
								// call this method itself to recreate subscription
								subscribeEventsJob();
							}, () -> {
								log.info("Finished reading events.");
							});
		}
	}
}
