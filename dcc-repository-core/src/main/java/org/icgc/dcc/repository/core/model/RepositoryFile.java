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
package org.icgc.dcc.repository.core.model;

import java.util.List;

import com.google.common.collect.Lists;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Repository file.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Uniform+metadata+JSON+document+for+ICGC+Data+Repositories
 */
@Data
@Accessors(chain = true)
public class RepositoryFile {

  String id;
  String fileId;
  List<String> study = Lists.newArrayList();
  String access;

  DataBundle dataBundle = new DataBundle();

  AnalysisMethod analysisMethod = new AnalysisMethod();

  DataCategorization dataCategorization = new DataCategorization();

  ReferenceGenome referenceGenome = new ReferenceGenome();

  List<FileCopy> fileCopies = Lists.newArrayList();

  List<Donor> donors = Lists.newArrayList();

  @Data
  @Accessors(chain = true)
  public static class DataBundle {

    String dataBundleId;

  }

  @Data
  @Accessors(chain = true)
  public static class AnalysisMethod {

    String analysisType;
    String software;

  }

  @Data
  @Accessors(chain = true)
  public static class DataCategorization {

    String dataType;
    String experimentalStrategy;

  }

  @Data
  @Accessors(chain = true)
  public static class ReferenceGenome {

    String genomeBuild;
    String referenceName;
    String downloadUrl;

  }

  @Data
  @Accessors(chain = true)
  public static class FileCopy {

    String fileName;
    String fileFormat;
    Long fileSize;
    String fileMd5sum;
    Long lastModified;
    IndexFile indexFile = new IndexFile();

    String repoType;
    String repoOrg; // TODO: Verify if this should be included
    String repoEntityId; // TODO: Verify if this should be included
    String repoName;
    String repoCode;
    String repoCountry;
    String repoBaseUrl;
    String repoDataPath;
    String repoMetadataPath;

  }

  @Data
  @Accessors(chain = true)
  public static class IndexFile {

    String fileId;
    String fileName;
    String fileFormat;
    Long fileSize;
    String fileMd5sum;

  }

  @Data
  @Accessors(chain = true)
  public static class Donor {

    String projectCode;
    String program;
    String study;
    String primarySite;

    String donorId;
    String specimenId;
    String specimenType;
    String sampleId;

    String submittedDonorId;
    String submittedSpecimenId;
    String submittedSampleId;

    OtherIdentifiers otherIdentifiers = new OtherIdentifiers();

    public boolean hasDonorId() {
      return donorId != null;
    }

  }

  @Data
  @Accessors(chain = true)
  public static class OtherIdentifiers {

    String tcgaParticipantBarcode;
    String tcgaSampleBarcode;
    String tcgaAliquotBarcode;

  }

}
