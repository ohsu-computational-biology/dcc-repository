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
package org.icgc.dcc.repository.core.model;

import static java.util.Collections.singletonList;

import java.util.List;

import com.google.common.collect.Lists;

import lombok.Data;
import lombok.val;
import lombok.experimental.Accessors;
import lombok.experimental.UtilityClass;

/**
 * Repository file.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Uniform+metadata+JSON+document+for+ICGC+Data+Repositories
 * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440
 */
@Data
@Accessors(chain = true)
public class RepositoryFile {

  String id;
  String objectId;
  List<String> study = Lists.newArrayList();
  String access;

  DataBundle dataBundle = new DataBundle();

  AnalysisMethod analysisMethod = new AnalysisMethod();

  DataCategorization dataCategorization = new DataCategorization();

  ReferenceGenome referenceGenome = new ReferenceGenome();

  List<FileCopy> fileCopies = Lists.newArrayList();

  List<Donor> donors = Lists.newArrayList();

  public Donor addDonor() {
    val donor = new Donor();
    donors.add(donor);

    return donor;
  }

  public FileCopy addFileCopy() {
    val fileCopy = new FileCopy();
    fileCopies.add(fileCopy);

    return fileCopy;
  }

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

    /**
     * Constants.
     */
    public static final ReferenceGenome PCAWG = new ReferenceGenome()
        .setDownloadUrl("ftp://ftp.sanger.ac.uk/pub/project/PanCancer/genome.fa.gz")
        .setGenomeBuild("GRCh37")
        .setReferenceName("hs37d5");

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

    String repoDataBundleId;
    String repoFileId;
    List<String> repoDataSetIds = Lists.newArrayList();

    String repoType;
    String repoOrg;
    String repoName;
    String repoCode;
    String repoCountry;
    String repoBaseUrl;
    String repoDataPath;
    String repoMetadataPath;

    public FileCopy setRepoDataSetId(String dataSetID) {
      return this.setRepoDataSetIds(singletonList(dataSetID));
    }

  }

  @Data
  @Accessors(chain = true)
  public static class IndexFile {

    String id;
    String objectId;
    String fileName;
    String fileFormat;
    Long fileSize;
    String fileMd5sum;

    String repoFileId;

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

    String matchedControlSampleId;

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

  //
  // Controlled vocabulary
  //

  @UtilityClass
  public class Study {

    public final String PCAWG = "PCAWG";

  }

  @UtilityClass
  public class Program {

    public final String TCGA = "TCGA";

  }

  @UtilityClass
  public class AnalysisType {

    public final String REFERENCE_ALIGNMENT = "Reference alignment"; // Using 'Variant calling' for ssm, cnsm, stsm etc
    public final String VARIANT_CALLING = "Variant calling";

  }

  @UtilityClass
  public class Software {

    public final String BWA_MEM = "BWA MEM";
    public final String STAR = "STAR";
    public final String TOP_HAT2 = "TopHat2";

    public final String BROAD_VARIANT_CALL_PIPELINE = "Broad variant call pipeline";
    public final String DKFZ_EMBL_VARIANT_CALL_PIPELINE = "DKFZ/EMBL variant call pipeline";
    public final String MUSE_VARIANT_CALL_PIPELINE = "MUSE variant call pipeline";
    public final String SANGER_VARIANT_CALL_PIPELINE = "Sanger variant call pipeline";

  }

  @UtilityClass
  public class FileFormat {

    public final String BAM = "BAM";
    public final String BAI = "BAI";
    public final String TBI = "TBI";
    public final String XML = "XML";
    public final String VCF = "VCF";
    public final String FASTQ = "FASTQ";

  }

  @UtilityClass
  public class FileAccess {

    public final String CONTROLLED = "controlled";
    public final String OPEN = "open";

  }

  @UtilityClass
  public class DataType {

    public final String CLINICAL = "Clinical";
    public final String ALIGNED_READS = "Aligned Reads";
    public final String UNALIGNED_READS = "Unaligned Reads";

    // These are for TCGA because we cannot determine aligned status from file
    public final String DNA_SEQ = "DNA-Seq";
    public final String RNA_SEQ = "RNA-Seq";

    public final String SSM = "SSM";
    public final String CNSM = "CNSM";
    public final String STSM = "StSM";
    public final String SGV = "SGV";
    public final String CNGV = "CNGV";
    public final String STGV = "StGV";

  }

  @UtilityClass
  public class ExperimentalStrategy {

    public final String WGS = "WGS";
    public final String RNA_SEQ = "RNA-Seq";

  }

}
