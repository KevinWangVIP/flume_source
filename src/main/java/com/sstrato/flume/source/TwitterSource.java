/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sstrato.flume.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * A Flume Source, which pulls data from Twitter's streaming API. Currently,
 * this only supports pulling from the sample API, and only gets new status
 * updates.
 */
public class TwitterSource extends AbstractSource implements EventDrivenSource,
		Configurable {

	private static final Logger logger = LoggerFactory
			.getLogger(TwitterSource.class);

	/** Twitter API 접근정 */
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;

	private String[] keywords;

	/** The actual Twitter stream. Json input포 */
	private final TwitterStream twitterStream = new TwitterStreamFactory(
			new ConfigurationBuilder().setJSONStoreEnabled(true).build())
			.getInstance();

	/**
	 * The initialization method for the Source. The context contains all the
	 * Flume configuration info, and can be used to retrieve any configuration
	 * values necessary to set up the Source.
	 */
	@Override
	public void configure(Context context) {
		// API접속 정보를 configuration 파일에서 읽어옴.
		consumerKey = context.getString("consumerKey");
		consumerSecret = context.getString("consumerSecret");
		accessToken = context.getString("accessToken");
		accessTokenSecret = context.getString("accessTokenSecret");
		String keywordString = context.getString("keyword");
		logger.info("Consumer Key:        '" + consumerKey + "'");
		logger.info("Consumer Secret:     '" + consumerSecret + "'");
		logger.info("Access Token:        '" + accessToken + "'");
		logger.info("Access Token Secret: '" + accessTokenSecret + "'");
		logger.info("keyword'"+keywordString+"'");
        
		//twitter API를 통해 읽어올 키워드 리스트 
		keywords = keywordString.split(",");
		for (int i = 0; i < keywords.length; i++) {
			keywords[i] = keywords[i].trim();
		}
	}

	/**
	 * Start processing events. This uses the Twitter Streaming API to sample
	 * Twitter, and process tweets.
	 */
	@Override
	public void start() {
		
		// 채널 등록
		final ChannelProcessor channel = getChannelProcessor();

		final Map<String, String> headers = new HashMap<String, String>();

		// 이 소스는 twitter4j의 StatusListener를 사용해서 트윗 스트림에 키워드가 들어오면 작동함. 
		
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes in.
			public void onStatus(Status status) {
				// header와 raw json 형태의 트윗으로 이벤트 구성.
			
				logger.debug(status.getUser().getScreenName() + ": "
						+ status.getText());

				headers.put("timestamp",
						String.valueOf(status.getCreatedAt().getTime()));
				Event event = EventBuilder.withBody(DataObjectFactory
						.getRawJSON(status).getBytes(), headers);

				channel.processEvent(event);
			}

			// This listener will ignore everything except for new tweets
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onScrubGeo(long userId, long upToStatusId) {
			}

			public void onException(Exception ex) {
			}
		};

		logger.debug(
				"Setting up Twitter sample stream using consumer key {} and"
						+ " access token {}", new String[] { this.consumerKey,
						this.accessToken });
		// Set up the stream's listener (defined above), and set any necessary
		// security information.
		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(this.consumerKey, this.consumerSecret);
		AccessToken token = new AccessToken(this.accessToken, this.accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);

		// Set up a filter to pull out industry-relevant tweets
		if (keywords.length == 0) {
			logger.debug("Starting up Twitter sampling...");
			twitterStream.sample();
		} else {
			logger.debug("Starting up Twitter filtering...");
			
			//필요한 키워드만을 트래킹함. 
			FilterQuery query = new FilterQuery().track(keywords)
					.setIncludeEntities(true);
			twitterStream.filter(query);
		}
		super.start();
	}

	/**
	 * Stops the Source's event processing and shuts down the Twitter stream.
	 */
	@Override
	public void stop() {
		logger.debug("Shutting down Twitter sample stream...");
		twitterStream.shutdown();
		super.stop();
	}
}
