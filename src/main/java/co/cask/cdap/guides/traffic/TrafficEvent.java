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

/**
 * Represents a report of current conditions from a given traffic sensor.
 */
public class TrafficEvent {
  public enum Type { VEHICLE, ACCIDENT };

  private final String roadSegmentId;
  private final long timestamp;
  private final Type type;
  private final int count;

  public TrafficEvent(String roadSegmentId, long timestamp, Type type, int count) {
    this.roadSegmentId = roadSegmentId;
    this.timestamp = timestamp;
    this.type = type;
    this.count = count;
  }

  public String getRoadSegmentId() {
    return roadSegmentId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Type getType() {
    return type;
  }

  public int getCount() {
    return count;
  }
}
