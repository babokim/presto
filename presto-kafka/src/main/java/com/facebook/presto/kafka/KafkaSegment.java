/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;

public class KafkaSegment
{
  private final int partitionId;
  private final long startOffset;
  private final long endOffset;
  private final HostAddress leaderNode;

  public KafkaSegment(
      int partitionId,
      long startOffset,
      long endOffset,
      HostAddress leaderNode)
  {
    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.leaderNode = leaderNode;
  }

  public int getPartitionId()
  {
    return partitionId;
  }

  public long getStartOffset()
  {
    return startOffset;
  }

  public long getEndOffset()
  {
    return endOffset;
  }

  public HostAddress getLeaderNode()
  {
    return leaderNode;
  }

  public String toString()
  {
    return "segment => {" + partitionId + ", " + startOffset + "-" + endOffset + ", " + leaderNode + "}";
  }
}
