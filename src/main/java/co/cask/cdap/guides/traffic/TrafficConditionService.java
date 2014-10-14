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
    useDataset(TrafficApp.TIMESERIES_TABLE_NAME);
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
    private static final long lookbackPeriod = TrafficApp.TIMESERIES_INTERVAL * 3;

    @UseDataSet(TrafficApp.TIMESERIES_TABLE_NAME)
    private CounterTimeseriesTable table;

    /**
     * Service method that determines a {@link co.cask.cdap.guides.traffic.TrafficConditionService.Condition}
     * corresponding to a given road segment for the most recent timeseries intervals:
     * <ul>
     *   <li>if the total number of traffic accidents is greater than 1, return RED;</li>
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
      long startTime = endTime - lookbackPeriod;

      byte[] roadSegmentId = Bytes.toBytes(segmentId);
      long accidentCount = 0;
      int congestedCount = 0;

      Iterator<CounterTimeseriesTable.Counter> accidentEvents =
        table.read(roadSegmentId, startTime, endTime, Bytes.toBytes(TrafficEvent.Type.ACCIDENT.name()));
      while (accidentEvents.hasNext()) {
        accidentCount += accidentEvents.next().getValue();
      }

      Condition currentCondition = Condition.GREEN;
      if (accidentCount > 0) {
        currentCondition = Condition.RED;
      } else {
        Iterator<CounterTimeseriesTable.Counter> vehicleEvents =
          table.read(roadSegmentId, startTime, endTime, Bytes.toBytes(TrafficEvent.Type.VEHICLE.name()));
        while (vehicleEvents.hasNext()) {
          if (vehicleEvents.next().getValue() > CONGESTED_THRESHOLD) {
            congestedCount++;
          }
        }
        if (congestedCount > 1) {
          currentCondition = Condition.RED;
        } else if (congestedCount > 0) {
          currentCondition = Condition.YELLOW;
        }
      }
      responder.sendString(currentCondition.name());
    }

    /**
     * Service method that returns the total of all vehicle counts reported over the recent timeseries records.
     */
    @Path("road/{segment}/vehicles")
    @GET
    public void vehicleCount(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("segment") String segmentId) {
      long endTime = System.currentTimeMillis();
      long startTime = endTime - lookbackPeriod;

      byte[] roadSegmentId = Bytes.toBytes(segmentId);
      long totalCount = 0;
      Iterator<CounterTimeseriesTable.Counter> events = table.read(roadSegmentId, startTime, endTime,
                                                                   Bytes.toBytes(TrafficEvent.Type.VEHICLE.name()));
      while (events.hasNext()) {
        totalCount += events.next().getValue();
      }
      responder.sendString(Long.toString(totalCount));
    }

    /**
     * Service method that returns the total of all accident counts reported over the recent timeseries records.
     */
    @Path("road/{segment}/accidents")
    @GET
    public void accidentCount(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("segment") String segmentId) {
      long endTime = System.currentTimeMillis();
      long startTime = endTime - lookbackPeriod;

      byte[] roadSegmentId = Bytes.toBytes(segmentId);
      long totalCount = 0;
      Iterator<CounterTimeseriesTable.Counter> events = table.read(roadSegmentId, startTime, endTime,
                                                                   Bytes.toBytes(TrafficEvent.Type.ACCIDENT.name()));
      while (events.hasNext()) {
        totalCount += events.next().getValue();
      }
      responder.sendString(Long.toString(totalCount));
    }
  }
}
