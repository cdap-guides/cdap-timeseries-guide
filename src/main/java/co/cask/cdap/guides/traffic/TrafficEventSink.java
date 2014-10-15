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
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CounterTimeseriesTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Increments timeseries counts for received {@link TrafficEvent}s per road segment ID and type.
 */
public class TrafficEventSink extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TrafficEventSink.class);

  @UseDataSet(TrafficApp.TIMESERIES_TABLE_NAME)
  private CounterTimeseriesTable table;

  @ProcessInput
  public void process(TrafficEvent event) {
    if (event.getCount() > 0) {
      table.increment(Bytes.toBytes(event.getRoadSegmentId()), event.getCount(), event.getTimestamp(),
                      Bytes.toBytes(event.getType().name()));
    } else {
      LOG.info("Skipping event with zero or negative count");
    }
  }
}
