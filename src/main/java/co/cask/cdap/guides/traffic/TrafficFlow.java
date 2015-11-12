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

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * Simple flow for processing and storing {@link TrafficEvent} records.
 */
public class TrafficFlow extends AbstractFlow {
  static final String FLOW_NAME = "TrafficFlow";

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("Reads traffic events from a stream and persists to a timeseries dataset");
    addFlowlet("parser", new TrafficEventParser());
    addFlowlet("sink", new TrafficEventSink());
    connectStream(TrafficApp.STREAM_NAME, "parser");
    connect("parser", "sink");
  }
}
