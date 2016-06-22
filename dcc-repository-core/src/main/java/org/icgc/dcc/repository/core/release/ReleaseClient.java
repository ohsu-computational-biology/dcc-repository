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
package org.icgc.dcc.repository.core.release;

import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.Joiners.COMMA;

import java.net.URL;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;

/**
 * @see https://jira.oicr.on.ca/browse/DCC-4843
 */
@RequiredArgsConstructor
public class ReleaseClient {

  /**
   * Constants.
   */
  private static final String DEFAULT_RELEASE_URL = "http://elasticsearch1.res.oicr.on.ca:9200/icgc-release";

  /**
   * Configuration.
   */
  private final String url;

  public ReleaseClient() {
    this(DEFAULT_RELEASE_URL);
  }

  public List<Donor> getDonors() {
    val result = readDonors();

    val hits = getHits(result);

    val donors = ImmutableList.<Donor> builder();
    for (val hit : hits) {
      val donor = createDonor(hit);
      donors.add(donor);
    }

    return donors.build();
  }

  @SneakyThrows
  private ObjectNode readDonors() {
    val size = 100_000; // Way greater than we expect (i.e. ~20k)
    val fields = COMMA.join("_id", "projectId", "submittedId"); // Limit fields to those needed
    val indexType = "donor-text"; // Small and has fields exposed
    val donorUrl = url + "/" + indexType + "/_search?size=" + size + "&fields=" + fields;

    return DEFAULT.readValue(new URL(donorUrl), ObjectNode.class);
  }

  private static Donor createDonor(JsonNode hit) {
    return Donor.builder()
        .donorId(hit.path("_id").textValue())
        .projectCode(hit.path("fields").path("projectId").get(0).textValue())
        .submittedDonorId(hit.path("fields").path("submittedId").get(0).textValue())
        .build();
  }

  private static JsonNode getHits(ObjectNode result) {
    return result.path("hits").path("hits");
  }

  @Value
  @Builder
  public static class Donor {

    String donorId;
    String projectCode;
    String submittedDonorId;

  }

}
