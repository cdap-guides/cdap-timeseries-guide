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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CounterTimeseriesTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.util.Iterator;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Service that queries the stored traffic events for the past 45 minutes to return a report on traffic
 * conditions.
 */
public class TrafficConditionService extends AbstractService {
  public enum Condition {GREEN, YELLOW, RED};

  static final String SERVICE_NAME = "TrafficConditions";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("Service to look up recent traffic conditions for a given road segment");
    addHandler(new TrafficConditionHandler());
  }

  /**
   * HTTP Service handler to lookup recent traffic conditions.
   */
  @Path("/v1")
  public static final class TrafficConditionHandler extends AbstractHttpServiceHandler {

    /** Threshold for number of vehicles in a time period, above which the road is considered congested */
    private static final int CONGESTED_THRESHOLD = 100;

    /** How far to look back for the traffic patterns */
    private static final long LOOKBACK_PERIOD = TrafficApp.TIMESERIES_INTERVAL * 3;

    @UseDataSet(TrafficApp.TIMESERIES_TABLE_NAME)
    private CounterTimeseriesTable table;

    /**
     * Service method that determines a {@link co.cask.cdap.guides.traffic.TrafficConditionService.Condition}
     * corresponding to a given road segment for the most recent timeseries intervals:
     * <ul>
     *   <li>if any traffic accidents were reported, return RED;</li>
     *   <li>if 2+ vehicle count reports are greater than the threshold, return RED;</li>
     *   <li>if 1 vehicle count report is greater than the threshold, return YELLOW;</li>
     *   <li>otherwise, return GREEN.</li>
     * </ul>
     */
    @Path("road/{segment}/recent")
    @GET
    public void recentConditions(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("segment") String segmentId) {
      long endTime = System.currentTimeMillis();
      long startTime = endTime - LOOKBACK_PERIOD;

      Condition currentCondition = Condition.GREEN;
      int accidentEntries =
        getCountsExceeding(segmentId, startTime, endTime, TrafficEvent.Type.ACCIDENT, 0);
      if (accidentEntries > 0) {
        currentCondition = Condition.RED;
      } else {
        int congestedEntries =
          getCountsExceeding(segmentId, startTime, endTime,
                             TrafficEvent.Type.VEHICLE, CONGESTED_THRESHOLD);
        if (congestedEntries > 1) {
          currentCondition = Condition.RED;
        } else if (congestedEntries > 0) {
          currentCondition = Condition.YELLOW;
        }
      }
      responder.sendString(currentCondition.name());
    }

    /**
     * Returns the number of counter entries with a value exceeding the given threshold.
     */
    private int getCountsExceeding(String roadSegmentId, long startTime, long endTime,
                                   TrafficEvent.Type type, long threshold) {
      int count = 0;
      Iterator<CounterTimeseriesTable.Counter> events =
        table.read(Bytes.toBytes(roadSegmentId), startTime, endTime, Bytes.toBytes(type.name()));
      while (events.hasNext()) {
        if (events.next().getValue() > threshold) {
          count++;
        }
      }
      return count;
    }
  }
}
