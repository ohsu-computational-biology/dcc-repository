/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.repository.client.index;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.repository.client.index.RepositoryFileIndex.INDEX_TYPE_FILE_DONOR_TEXT_NAME;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.icgc.dcc.repository.core.model.RepositoryFileCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.mongodb.MongoClientURI;

import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.experimental.Accessors;

public class FileDonorDocumentProcessor extends DocumentProcessor {

  /**
   * Constants.
   */
  private static final List<String> FIELD_NAMES = ImmutableList.of(
      "specimen_id",
      "sample_id",
      "submitted_specimen_id",
      "submitted_sample_id",
      "tcga_participant_barcode",
      "tcga_sample_barcode",
      "tcga_aliquot_barcode");

  public FileDonorDocumentProcessor(MongoClientURI mongoUri, String indexName, BulkProcessor bulkProcessor) {
    super(mongoUri, indexName, bulkProcessor);
  }

  @Override
  @SneakyThrows
  public int process() {
    val summary = resolveFileDonorSummary();

    // Index
    for (val donorId : summary.donorIds()) {
      val fileDonor = createFileDonor(summary, donorId);

      add(INDEX_TYPE_FILE_DONOR_TEXT_NAME, donorId, fileDonor);
    }

    return summary.donorIds().size();
  }

  private FileDonorSummary resolveFileDonorSummary() {
    val summary = new FileDonorSummary();

    // Collect
    eachDocument(RepositoryFileCollection.FILE, file -> {
      ArrayNode donors = file.withArray("donors");
      for (JsonNode donor : donors) {
        String donorId = donor.get("donor_id").textValue();
        summary.donorIds().add(donorId);

        String submittedDonorId = donor.get("submitted_donor_id").textValue();
        summary.submittedDonorIds().put(donorId, submittedDonorId);

        for (String fieldName : FIELD_NAMES) {
          String value = resolveFieldValue(donor, fieldName);
          if (!isNullOrEmpty(value)) {
            Multimap<String, String> values = summary.donorFields().get(fieldName);
            values.put(donorId, value);
          }
        }
      }
    });

    return summary;
  }

  private String resolveFieldValue(JsonNode donor, String fieldName) {
    return fieldName.startsWith("tcga") ? donor.get("other_identifiers").get(fieldName).textValue() : donor
        .get(fieldName).textValue();
  }

  private ObjectNode createFileDonor(FileDonorSummary summary, String donorId) {
    val fileDonor = createDocument();
    fileDonor.put("id", donorId);
    fileDonor.put("donor_id", donorId);

    val submittedDonorId = summary.submittedDonorIds().get(donorId);
    if (!isNullOrEmpty(submittedDonorId)) {
      fileDonor.put("submitted_donor_id", submittedDonorId);
    }

    for (val fieldName : FIELD_NAMES) {
      fileDonor.putPOJO(fieldName, summary.donorFields().get(fieldName).get(donorId));
    }

    return fileDonor;
  }

  @Value
  @Accessors(fluent = true)
  private static class FileDonorSummary {

    Set<String> donorIds = Sets.<String> newHashSet();
    Map<String, String> submittedDonorIds = Maps.newHashMap();
    Map<String, Multimap<String, String>> donorFields = Maps.newHashMap();

    {
      for (val fieldName : FIELD_NAMES) {
        donorFields.put(fieldName, HashMultimap.<String, String> create());
      }
    }

  }

}
