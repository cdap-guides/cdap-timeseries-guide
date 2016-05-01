/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.guides.traffic;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Parses stream events for traffic sensor events in the format:
 * {@code <segmentID>, <timestamp>, <type>, <count>} (comma-separated values), where each of the fields are:
 * <ul>
 *   <li>{@code segmentID} - a unique string identifier for the given road segment the event applies to</li>
 *   <li>{@code timestamp} - the timestamp for the event in {@code yyyy-MM-dd HH:mm:ss} format</li>
 *   <li>{@code type} - the type of event (VEHICLE|ACCIDENT)</li>
 *   <li>{@code count} - the count of incidents to report for the event.</li>
 * </ul>
 */
public class TrafficEventParser extends AbstractFlowlet {
  /** Special timestamp string used to indicate that the current timestamp should be used */
  public static final String TIMESTAMP_NOW = "now";
  /** Expected date format for the timestamp strings */
  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  /** Metrics key used to count bad records received from the stream */
  private static final String BAD_EVENT = "event.bad";

  private static final Logger LOG = LoggerFactory.getLogger(TrafficEventParser.class);

  private final DateFormat df = new SimpleDateFormat(DATE_FORMAT);

  private Metrics metrics;
  private OutputEmitter<TrafficEvent> out;

  @ProcessInput
  public void process(StreamEvent event) {
    String body = Charsets.UTF_8.decode(event.getBody()).toString();
    String[] parts = body.split("\\s*,\\s*");
    if (parts.length != 4) {
      LOG.info("Received a malformed event message: {}", body);
      metrics.count(BAD_EVENT, 1);
      return;
    }

    long timestamp;
    try {
      if (TIMESTAMP_NOW.equalsIgnoreCase(parts[1])) {
        timestamp = System.currentTimeMillis();
      } else {
        timestamp = df.parse(parts[1]).getTime();
      }
    } catch (ParseException pe) {
      LOG.info("Timestamp should be in 'yyyy-MM-dd HH:mm:ss' format, got: {}", parts[1]);
      metrics.count(BAD_EVENT, 1);
      return;
    }
    TrafficEvent.Type type;
    try {
      type = TrafficEvent.Type.valueOf(parts[2]);
    } catch (IllegalArgumentException iae) {
      LOG.info("Type should be 'VEHICLE' or 'ACCIDENT', got: {}", parts[2]);
      metrics.count(BAD_EVENT, 1);
      return;
    }
    int count;
    try {
      count = Integer.parseInt(parts[3]);
    } catch (NumberFormatException nfe) {
      LOG.info("Invalid integer for count, got: {}", parts[3]);
      metrics.count(BAD_EVENT, 1);
      return;
    }

    out.emit(new TrafficEvent(parts[0], timestamp, type, count));
  }
}
