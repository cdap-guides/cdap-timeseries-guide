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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CounterTimeseriesTable;
import co.cask.cdap.api.dataset.lib.TimeseriesTables;

/**
 * An application that demonstrates real-time processing of events from a sensor network, and storage using
 * a timeseries Dataset.
 */
public class TrafficApp extends AbstractApplication {
  static final String APP_NAME = "TrafficApp";
  static final String STREAM_NAME = "trafficEvents";
  static final String TIMESERIES_TABLE_NAME = "trafficEventTable";

  /**
   * Time interval to store per row in the TimeseriesTable.  This controls the time range over which entries
   * are grouped together for efficient querying.
   */
  public static final int TIMESERIES_INTERVAL = 15 * 60 * 1000; // 15 minutes

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Traffic event data processing");
    addStream(new Stream(STREAM_NAME));

    // configure the timeseries table
    DatasetProperties props = TimeseriesTables.timeseriesTableProperties(TIMESERIES_INTERVAL, DatasetProperties.EMPTY);
    createDataset(TIMESERIES_TABLE_NAME, CounterTimeseriesTable.class, props);

    addFlow(new TrafficFlow());
    addService(new TrafficConditionService());
  }
}
