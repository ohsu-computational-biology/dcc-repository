/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.repository.cloud.s3;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CloudS3BucketReader {

  /**
   * Configuration.
   */
  @NonNull
  private final String bucketName;
  @NonNull
  private final String prefix;

  /**
   * Dependencies.
   */
  @NonNull
  private final AmazonS3 s3;

  public List<S3ObjectSummary> readSummaries() {
    val objectSummaries = ImmutableList.<S3ObjectSummary> builder();

    // For all bucket partitions
    for (val bucketName : getBucketNames()) {
      readBucket(bucketName, prefix, (objectSummary) -> {
        objectSummaries.add(objectSummary);
      });
    }

    return objectSummaries.build();
  }

  private void readBucket(String bucketName, String prefix, Consumer<S3ObjectSummary> callback) {
    val request = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix);
    log.info("Reading summaries from '{}/{}'...", bucketName, prefix);

    ObjectListing listing;
    do {
      listing = s3.listObjects(request);
      for (val objectSummary : listing.getObjectSummaries()) {
        callback.accept(objectSummary);
      }

      request.setMarker(listing.getNextMarker());
    } while (listing.isTruncated());
  }

  private Set<String> getBucketNames() {
    val bucketNames = ImmutableSet.<String> builder();

    for (val bucketPartition : s3.listBuckets()) {
      if (isBucketPartition(bucketPartition)) {
        bucketNames.add(bucketPartition.getName());
      }
    }

    return bucketNames.build();
  }

  private boolean isBucketPartition(Bucket bucketPartition) {
    // Bucket partitioning naming scheme
    val bucketPartitionPattern = Pattern.quote(bucketName) + "(\\.\\d+)?";
    return bucketPartition.getName().matches(bucketPartitionPattern);
  }

}
