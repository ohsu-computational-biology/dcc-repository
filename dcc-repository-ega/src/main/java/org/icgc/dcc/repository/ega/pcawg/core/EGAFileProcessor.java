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
package org.icgc.dcc.repository.ega.pcawg.core;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.repository.ega.pcawg.util.EGAAnalysisFiles.getChecksum;
import static org.icgc.dcc.repository.ega.pcawg.util.EGAAnalysisFiles.getFiles;
import static org.icgc.dcc.repository.ega.pcawg.util.EGAAnalysisFiles.getSampleRef;
import static org.icgc.dcc.repository.ega.pcawg.util.EGASampleFiles.getSampleAlias;
import static org.icgc.dcc.repository.ega.pcawg.util.EGASampleFiles.getSampleRefName;
import static org.icgc.dcc.repository.ega.pcawg.util.EGAStudyFiles.getAccession;

import java.util.List;
import java.util.Optional;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.AnalysisMethod;
import org.icgc.dcc.repository.core.model.RepositoryFile.AnalysisType;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataCategorization;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataType;
import org.icgc.dcc.repository.core.model.RepositoryFile.ExperimentalStrategy;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryFile.ReferenceGenome;
import org.icgc.dcc.repository.core.model.RepositoryFile.Software;
import org.icgc.dcc.repository.core.model.RepositoryFile.Study;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.repository.ega.pcawg.model.EGAAnalysisFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGAGnosFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGAStudyFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGASubmission;
import org.icgc.dcc.repository.ega.pcawg.util.EGAAnalysisFiles;
import org.icgc.dcc.repository.ega.pcawg.util.EGAGnosFiles;
import org.icgc.dcc.repository.ega.pcawg.util.EGASampleFiles;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EGAFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  private final RepositoryServer egaServer;

  public EGAFileProcessor(RepositoryFileContext context, @NonNull RepositoryServer egaServer) {
    super(context);
    this.egaServer = egaServer;
  }

  public Iterable<RepositoryFile> processSubmissions(@NonNull Iterable<EGASubmission> submissions) {
    return stream(submissions)
        .flatMap(stream(this::processSubmission))
        .peek(f -> log.debug("{}", f))
        .collect(toImmutableList());
  }

  private List<RepositoryFile> processSubmission(EGASubmission submission) {

    //
    // Prepare
    //

    val studyFile = submission.getStudyFile();
    val analysisFile = submission.getAnalysisFile();
    val gnosFile = submission.getGnosFile();

    val projectCode = analysisFile.getProjectId();
    val analysisId = analysisFile.getAnalysisId();

    val files = getFiles(analysisFile);
    val sampleAttributes = resolveSampleAttributes(submission);
    val project = getProjectCodeProject(projectCode).get();

    //
    // Create
    //

    val egaFiles = ImmutableList.<RepositoryFile> builder();
    for (val file : files) {
      if (isExcludedFile(file)) {
        continue;
      }

      if (!isBamFile(file)) {
        context.reportWarning("Skipping unexpected non-BAM submission file: %s", file);
        continue;
      }

      val fileName = resolveFileName(file);
      val objectId = resolveObjectId(analysisId, fileName);

      // TODO: Add support for *.tbi, *.idx as they come online
      val baiFile = resolveBaiFile(files, file);

      val egaFile = new RepositoryFile()
          .setId(context.ensureFileId(objectId))
          .setObjectId(objectId)
          .setStudy(resolveStudies(studyFile))
          .setAccess(FileAccess.CONTROLLED);

      egaFile.getDataBundle()
          .setDataBundleId(analysisId);

      egaFile.setDataCategorization(resolveDataCategorization(analysisFile, fileName));

      egaFile.setAnalysisMethod(resolveAnalysisMethod(analysisFile));

      egaFile.setReferenceGenome(ReferenceGenome.PCAWG);

      val fileCopy = egaFile.addFileCopy()
          .setFileName(fileName)
          .setFileFormat(resolveFileFormat(file))
          .setFileSize(resolveFileSize(fileName, gnosFile))
          .setFileMd5sum(getChecksum(file))
          .setLastModified(resolveLastModified(submission))
          .setRepoDataBundleId(null) // TODO: Resolve
          .setRepoFileId(null) // TODO: Resolve
          .setRepoType(egaServer.getType().getId())
          .setRepoOrg(egaServer.getSource().getId())
          .setRepoName(egaServer.getName())
          .setRepoCode(egaServer.getCode())
          .setRepoCountry(egaServer.getCountry())
          .setRepoBaseUrl(egaServer.getBaseUrl())
          .setRepoMetadataPath(egaServer.getType().getMetadataPath())
          .setRepoDataPath(egaServer.getType().getDataPath());

      if (baiFile.isPresent()) {
        val baiFileName = resolveFileName(baiFile.get());
        val baiObjectId = resolveObjectId(analysisId, baiFileName);
        fileCopy.getIndexFile()
            .setId(context.ensureFileId(baiObjectId))
            .setObjectId(baiObjectId)
            .setRepoFileId(null) // TODO: Resolve
            .setFileName(baiFileName)
            .setFileFormat(FileFormat.BAI)
            .setFileSize(resolveFileSize(baiFileName, gnosFile))
            .setFileMd5sum(getChecksum(baiFile.get()));
      }

      egaFile.addDonor()
          .setPrimarySite(context.getPrimarySite(projectCode))
          .setProjectCode(projectCode)
          .setProgram(project.getProgram())
          .setStudy(Study.PCAWG)
          .setDonorId(resolveDonorId(sampleAttributes))
          .setSpecimenId(resolveSpecimenId(sampleAttributes))
          .setSpecimenType(resolveSpecimenType(sampleAttributes))
          .setSampleId(resolveSampleId(sampleAttributes))
          .setSubmittedDonorId(resolveSubmitterDonorId(sampleAttributes))
          .setSubmittedSpecimenId(resolveSubmitterSpecimenId(sampleAttributes))
          .setSubmittedSampleId(resolveSubmitterSampleId(sampleAttributes))
          .setMatchedControlSampleId(null) // TODO: Address when non-alignment types are present
          .setOtherIdentifiers(null); // N/A for non-TCGA

      egaFiles.add(egaFile);
    }

    return egaFiles.build();
  }

  //
  // Utilities
  //

  private static boolean isExcludedFile(JsonNode file) {
    return isIndexFile(file) || isReadmeFile(file);
  }

  private static boolean isIndexFile(JsonNode file) {
    return isBaiFile(file) || isTbiFile(file) || isIdxFile(file);
  }

  private static boolean isBamFile(JsonNode file) {
    return hasFileExtension(file, ".bam");
  }

  private static boolean isBaiFile(JsonNode file) {
    return hasFileExtension(file, ".bai");
  }

  private static boolean isTbiFile(JsonNode file) {
    return hasFileExtension(file, ".tbi");
  }

  private static boolean isIdxFile(JsonNode file) {
    return hasFileExtension(file, ".idx");
  }

  private static boolean isReadmeFile(JsonNode file) {
    return hasFileExtension(file, ".GNOS.xml.gz");
  }

  private static boolean hasFileExtension(JsonNode file, String extension) {
    return resolveFileName(file).endsWith(extension);
  }

  private static boolean isBWAAlignment(EGAAnalysisFile analysisFile) {
    return analysisFile.getType().equals("alignment") && analysisFile.getWorkflow().equals("WGS_BWA");
  }

  private static List<String> resolveStudies(EGAStudyFile studyFile) {
    return studies(studyFile.getStudy(), getAccession(studyFile));
  }

  private static DataCategorization resolveDataCategorization(@NonNull EGAAnalysisFile analysisFile,
      @NonNull String fileName) {
    val category = new DataCategorization();

    // [1] = See PCAWGFileInfoResolver for approach when new file types come online
    val rnaAlignment = false; // TODO: [1]
    val variantCalling = false; // TODO: [1]
    if (rnaAlignment) {
      category
          .setDataType(DataType.ALIGNED_READS)
          .setExperimentalStrategy(ExperimentalStrategy.RNA_SEQ);
    } else if (isBWAAlignment(analysisFile)) {
      category
          .setDataType(DataType.ALIGNED_READS)
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    } else if (variantCalling) {
      category
          .setDataType(null) // TODO: [1]
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    }

    return category;
  }

  private static AnalysisMethod resolveAnalysisMethod(@NonNull EGAAnalysisFile analysisFile) {
    val analysisMethod = new AnalysisMethod();

    // [1] = See PCAWGFileInfoResolver for approach when new file types come online
    val rnaAlignment = false; // TODO: [1]
    val variantCalling = false; // TODO: [1]

    if (rnaAlignment) {
      analysisMethod
          .setAnalysisType(AnalysisType.REFERENCE_ALIGNMENT)
          .setSoftware(null); // TODO: [1]
    } else if (isBWAAlignment(analysisFile)) {
      analysisMethod
          .setAnalysisType(AnalysisType.REFERENCE_ALIGNMENT)
          .setSoftware(Software.BWA_MEM);
    } else if (variantCalling) {
      analysisMethod
          .setAnalysisType(AnalysisType.VARIANT_CALLING)
          .setSoftware(null); // TODO: [1]
    }

    return analysisMethod;
  }

  private static String resolveFileName(JsonNode file) {
    val analysisAndFileName = EGAAnalysisFiles.getFileName(file);
    return analysisAndFileName.split("/")[1];
  }

  private static String resolveFileFormat(JsonNode file) {
    val fileType = EGAAnalysisFiles.getFileType(file);
    if (fileType.equals("bam")) {
      return FileFormat.BAM;
    }

    // TODO: Add when new file types come online
    return null;
  }

  private static Long resolveFileSize(String fileName, EGAGnosFile gnosFile) {
    val files = EGAGnosFiles.getFiles(gnosFile);

    return stream(files)
        .filter(f -> EGAGnosFiles.getFileName(f).equals(fileName))
        .map(f -> EGAGnosFiles.getFileSize(f))
        .findFirst()
        .orElse(null);
  }

  private static long resolveLastModified(EGASubmission submission) {
    return submission.getReceiptFile().getTimestamp();
  }

  private static Optional<JsonNode> resolveBaiFile(ArrayNode files, JsonNode file) {
    val baiFileName = resolveFileName(file) + ".bai";

    return stream(files)
        .filter(f -> resolveFileName(f).equals(baiFileName))
        .findFirst();
  }

  private static ObjectNode resolveSampleAttributes(EGASubmission submission) {
    val sampleRef = getSampleRef(submission.getAnalysisFile());
    val sampleRefName = getSampleRefName(sampleRef);

    return submission.getSampleFiles().stream()
        .flatMap(stream(EGASampleFiles::getSamples))
        .filter(s -> sampleRefName.equals(getSampleAlias(s))) // SRA convention
        .map(EGASampleFiles::getSampleAttributes)
        .findFirst()
        .orElse(null);
  }

  private static String resolveDonorId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("icgc_donor_id").textValue();
  }

  private static String resolveSpecimenId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("icgc_specimen_id").textValue();
  }

  private static String resolveSampleId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("icgc_sample_id").textValue();
  }

  private static String resolveSpecimenType(ObjectNode sampleAttributes) {
    return sampleAttributes.get("specimen_type").textValue();
  }

  private static String resolveSubmitterDonorId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("submitter_donor_id").asText(); // Can be integer!
  }

  private static String resolveSubmitterSpecimenId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("submitter_specimen_id").textValue();
  }

  private static String resolveSubmitterSampleId(ObjectNode sampleAttributes) {
    return sampleAttributes.get("submitter_sample_id").textValue();
  }

}
