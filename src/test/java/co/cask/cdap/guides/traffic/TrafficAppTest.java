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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Tests covering the {@link TrafficApp} application.
 */
public class TrafficAppTest extends TestBase {
  private DateFormat df = new SimpleDateFormat(TrafficEventParser.DATE_FORMAT);

  private int streamEventCount = 0;

  @Test
  public void testApp() throws Exception {
    // Deploy the application
    ApplicationManager appManager = deployApplication(TrafficApp.class);

    // Start the flow
    FlowManager flowManager = appManager.startFlow(TrafficFlow.FLOW_NAME);
    try {
      String segment1 = "66N_1";
      String segment2 = "66N_2";
      String segment3 = "66N_3";
      String segment4 = "66N_4";

      // Load some data for querying
      StreamWriter streamWriter = appManager.getStreamWriter(TrafficApp.STREAM_NAME);
      long now = System.currentTimeMillis();

      // Segment 1 only has entries below the threshold (threshold is 100)
      sendRecord(streamWriter, segment1, now - TrafficApp.TIMESERIES_INTERVAL, TrafficEvent.Type.VEHICLE, 10);
      sendRecord(streamWriter, segment1, now, TrafficEvent.Type.VEHICLE, 10);

      // Segment 2 has one timeseries interval over the threshold
      sendRecord(streamWriter, segment2, now - (2 * TrafficApp.TIMESERIES_INTERVAL), TrafficEvent.Type.VEHICLE, 101);
      sendRecord(streamWriter, segment2, now - TrafficApp.TIMESERIES_INTERVAL, TrafficEvent.Type.VEHICLE, 10);

      // Segment 3 has two timeseries intervals over the threshold
      sendRecord(streamWriter, segment3, now - (2 * TrafficApp.TIMESERIES_INTERVAL), TrafficEvent.Type.VEHICLE, 101);
      sendRecord(streamWriter, segment3, now - TrafficApp.TIMESERIES_INTERVAL, TrafficEvent.Type.VEHICLE, 10);
      // summed records for the same timestamp will combine over the threshold of 100
      sendRecord(streamWriter, segment3, now, TrafficEvent.Type.VEHICLE, 51);
      sendRecord(streamWriter, segment3, now, TrafficEvent.Type.VEHICLE, 51);

      // Segment 4 has an accident
      sendRecord(streamWriter, segment4, now - TrafficApp.TIMESERIES_INTERVAL, TrafficEvent.Type.VEHICLE, 10);
      sendRecord(streamWriter, segment4, now - TrafficApp.TIMESERIES_INTERVAL, TrafficEvent.Type.ACCIDENT, 1);
      sendRecord(streamWriter, segment4, now, TrafficEvent.Type.VEHICLE, 10);

      // Wait until all stream events have been processed by the TrafficEventStore Flowlet
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics(TrafficApp.APP_NAME, TrafficFlow.FLOW_NAME, "sink");
      metrics.waitForProcessed(streamEventCount, 5, TimeUnit.SECONDS);

      ServiceManager serviceManager = appManager.startService(TrafficConditionService.SERVICE_NAME);
      try {
        serviceManager.waitForStatus(true);
        URL url = serviceManager.getServiceURL();
        // Segment 1 should be GREEN, since no intervals are over threshold
        assertSegmentStatus(url, segment1, TrafficConditionService.Condition.GREEN);
        // Segment 2 should be YELLOW, since 1 interval exceeds the threshold
        assertSegmentStatus(url, segment2, TrafficConditionService.Condition.YELLOW);
        // Segment 3 should be RED, since 2 intervals exceed the threshold
        assertSegmentStatus(url, segment3, TrafficConditionService.Condition.RED);
        // Segment 4 should be RED, since there is an accident in one interval
        assertSegmentStatus(url, segment4, TrafficConditionService.Condition.RED);
      } finally {
        serviceManager.stop();
        serviceManager.waitForStatus(false);
      }
    } finally {
      flowManager.stop();
    }
  }

  /**
   * Sends a record for the given fields to the application's input stream.
   */
  private void sendRecord(StreamWriter streamWriter, String segment, long timestamp, TrafficEvent.Type type,
                          int count) throws IOException {
    // Expected format for stream records: <segment>, <timestamp>, <type>, <count>
    String streamFormat = "%s, %s, %s, %d";
    streamWriter.send(String.format(streamFormat, segment, df.format(new Date(timestamp)), type.name(), count));
    streamEventCount++;
  }

  /**
   * Checks that the status returned for a given road segment matches the expected value.  This check will retry
   * a set number of tries (with a pause between attempts) in order to account for delays in the stream events being
   * recorded.
   */
  private void assertSegmentStatus(URL serviceUrl, String segment,
                                   TrafficConditionService.Condition expectedCondition) throws IOException {
    URL url = new URL(serviceUrl, String.format("v1/road/%s/recent", segment));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(expectedCondition.name(), response.getResponseBodyAsString());
  }
}
