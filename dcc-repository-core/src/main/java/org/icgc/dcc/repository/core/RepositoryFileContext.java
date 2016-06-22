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
package org.icgc.dcc.repository.core;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.repository.core.util.RepositoryFiles.qualifyDonorId;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.icgc.dcc.common.core.report.BufferedReport;
import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.id.client.core.IdClient;
import org.icgc.dcc.repository.core.model.RepositorySource;

import com.mongodb.MongoClientURI;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor(access = PACKAGE)
public class RepositoryFileContext {

  /**
   * Configuration.
   */
  @Getter
  @NonNull
  private final MongoClientURI mongoUri;
  @Getter
  @NonNull
  private final URI esUri;
  @Getter
  @NonNull
  private final String indexAlias;
  @Getter
  private final boolean skipImport;
  @Getter
  @NonNull
  private final Set<RepositorySource> sources;
  @Getter
  private final boolean readOnly;

  /**
   * Metadata.
   */
  @NonNull
  private final Map<String, String> primarySites;

  /**
   * Dependencies.
   */
  @NonNull
  private final IdClient idClient;
  @NonNull
  private final TCGAClient tcgaClient;
  @NonNull
  private final RepositoryIdResolver pcawgIdResolver;
  private final RepositoryIdResolver dccIdResolver;
  @Getter
  @NonNull
  private final BufferedReport report;

  /**
   * Data.
   */
  @Getter(lazy = true, value = PRIVATE)
  private final Set<String> pcawgSubmittedDonorIds = pcawgIdResolver.resolveIds();
  @Getter(lazy = true, value = PRIVATE)
  private final Set<String> dccSubmittedDonorIds = dccIdResolver.resolveIds();

  public boolean isSourceActive(@NonNull RepositorySource source) {
    return sources.contains(source);
  }

  public void reportError(String error, Object... args) {
    report.addError(error, args);
  }

  public void reportWarning(String warning, Object... args) {
    report.addWarning(warning, args);
  }

  public String getPrimarySite(@NonNull String projectCode) {
    return primarySites.get(projectCode);
  }

  public Map<String, String> getTCGAUUIDs(@NonNull Set<String> tcgaBarcodes) {
    return tcgaClient.getUUIDs(tcgaBarcodes);
  }

  public Map<String, String> getTCGABarcodes(@NonNull Set<String> tcgaUuids) {
    return tcgaClient.getBarcodes(tcgaUuids);
  }

  public boolean isDCCSubmittedDonorId(@NonNull String projectCode, @NonNull String submittedDonorId) {
    if (getDccSubmittedDonorIds().contains(qualifyDonorId(projectCode, submittedDonorId))) {
      return true;
    }

    // Special case for TCGA and TARGET projects that submit legacy barcodes to DCC but UUIDs everywhere else
    val translatedSubmittedDonorId = tcgaClient.getBarcode(submittedDonorId);
    if (getDccSubmittedDonorIds().contains(qualifyDonorId(projectCode, translatedSubmittedDonorId))) {
      return true;
    }

    return false;
  }

  public boolean isPCAWGSubmittedDonorId(@NonNull String projectCode, @NonNull String submittedDonorId) {
    return getPcawgSubmittedDonorIds().contains(qualifyDonorId(projectCode, submittedDonorId));
  }

  public String getDonorId(@NonNull String submittedDonorId, @NonNull String submittedProjectId) {
    return idClient.getDonorId(submittedDonorId, submittedProjectId).orElse(null);
  }

  public String ensureDonorId(@NonNull String submittedDonorId, @NonNull String submittedProjectId) {
    try {
      if (readOnly) {
        return getDonorId(submittedDonorId, submittedProjectId);
      }

      return idClient.createDonorId(submittedDonorId, submittedProjectId);
    } catch (Exception e) {
      throw new RuntimeException("Error ensuring donor id for submittedDonorId=" + submittedDonorId
          + ", submittedProjectId=" + submittedProjectId, e);
    }
  }

  public String getSpecimenId(@NonNull String submittedSpecimenId, @NonNull String submittedProjectId) {
    return idClient.getSpecimenId(submittedSpecimenId, submittedProjectId).orElse(null);
  }

  public String ensureSpecimenId(@NonNull String submittedSpecimenId, @NonNull String submittedProjectId) {
    try {
      if (readOnly) {
        return getSpecimenId(submittedSpecimenId, submittedProjectId);
      }

      return idClient.createSpecimenId(submittedSpecimenId, submittedProjectId);

    } catch (Exception e) {
      throw new RuntimeException("Error ensuring specimen id for submittedSpecimenId=" + submittedSpecimenId
          + ", submittedProjectId=" + submittedProjectId, e);
    }
  }

  public String getSampleId(@NonNull String submittedSampleId, @NonNull String submittedProjectId) {
    return idClient.getSampleId(submittedSampleId, submittedProjectId).orElse(null);
  }

  public String ensureSampleId(@NonNull String submittedSampleId, @NonNull String submittedProjectId) {
    try {
      if (readOnly) {
        return getSampleId(submittedSampleId, submittedProjectId);
      }

      return idClient.createSampleId(submittedSampleId, submittedProjectId);
    } catch (Exception e) {
      throw new RuntimeException("Error ensuring sample id for submittedSampleId=" + submittedSampleId
          + ", submittedProjectId=" + submittedProjectId, e);
    }
  }

  public String ensureFileId(@NonNull String objectId) {
    try {
      if (readOnly) {
        return getFileId(objectId);
      }

      return idClient.createFileId(objectId);
    } catch (Exception e) {
      throw new RuntimeException("Error ensuring file id for objectId=" + objectId, e);
    }
  }

  public String getFileId(@NonNull String submittedFileId) {
    return idClient.getFileId(submittedFileId).orElse(null);
  }

}