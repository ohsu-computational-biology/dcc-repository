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
package org.icgc.dcc.repository.cghub.core;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getAliquotId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getAnalysisId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getAnalyteCode;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getChecksum;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getDiseaseAbbr;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getFileName;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getFileSize;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getFiles;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getLastModified;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getLegacyDonorId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getLegacySampleId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getLegacySpecimenId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getLibraryStrategy;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getParticipantId;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getResults;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getSampleId;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getDiseaseCodeProject;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getCGHubServer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataType;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryProject;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CGHubFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final List<String> DNA_SEQ_ANALYTE_CODES = ImmutableList.of("D", "G", "W", "X");
  private static final List<String> RNA_SEQ_ANALYTE_CODES = ImmutableList.of("R", "T", "H");

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer cghubServer = getCGHubServer();

  public CGHubFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  @NonNull
  public Iterable<RepositoryFile> processDetails(Iterable<ObjectNode> details) {
    log.info("Procesing and filtering details...");
    val analysisFiles = stream(details)
        .flatMap(detail -> stream(getResults(detail)))
        .flatMap(result -> stream(processResult(result)))
        .filter(hasDonorId()) // Filter out non-ICGC donors
        .collect(toImmutableList());

    log.info("Assigning study...");
    assignStudy(analysisFiles);

    return analysisFiles;
  }

  private Iterable<RepositoryFile> processResult(JsonNode result) {
    val baiFile = resolveBaiFile(result);

    // TODO: Consider relaxing this to include FASTQ/A files. Talk with JJ about what we ones we want to keep
    // (interesting files).
    // May need to see if we can identify FASTQ based on file name, other fields, etc.
    return resolveFiles(result, file -> isBamFile(file))
        .map(file -> createAnalysisFile(result, file, baiFile))
        .collect(toImmutableList());
  }

  private RepositoryFile createAnalysisFile(JsonNode result, JsonNode file, Optional<JsonNode> baiFile) {

    //
    // Prepare
    //

    val project = resolveProject(result);
    val projectCode = project.getProjectCode();

    val legacySampleId = getLegacySampleId(result);
    val legacySpecimenId = getLegacySpecimenId(legacySampleId);
    val legacyDonorId = getLegacyDonorId(legacySampleId);

    val analysisId = getAnalysisId(result);
    val fileName = getFileName(file);
    val id = resolveId(analysisId, fileName);

    //
    // Create
    //

    val analysisFile = new RepositoryFile()
        .setId(id)
        .setFileId(context.ensureFileId(id))
        .setStudy(null) // N/A
        .setAccess(FileAccess.CONTROLLED);

    analysisFile.getDataBundle()
        .setDataBundleId(analysisId);

    analysisFile.getDataCategorization()
        .setDataType(resolveDataType(result))
        .setExperimentalStrategy(getLibraryStrategy(result));

    val fileCopy = analysisFile.addFileCopy()
        .setFileName(fileName)
        .setFileFormat(FileFormat.BAM)
        .setFileSize(getFileSize(file))
        .setFileMd5sum(getChecksum(file))
        .setLastModified(resolveLastModified(result))
        .setRepoType(cghubServer.getType().getId())
        .setRepoOrg(cghubServer.getSource().getId())
        .setRepoName(cghubServer.getName())
        .setRepoCode(cghubServer.getCode())
        .setRepoCountry(cghubServer.getCountry())
        .setRepoBaseUrl(cghubServer.getBaseUrl())
        .setRepoMetadataPath(cghubServer.getType().getMetadataPath())
        .setRepoDataPath(cghubServer.getType().getDataPath());

    if (baiFile.isPresent()) {
      val baiFileName = getFileName(baiFile.get());
      val baiId = resolveId(analysisId, baiFileName);
      fileCopy.getIndexFile()
          .setId(baiId)
          .setFileId(context.ensureFileId(baiId))
          .setFileName(baiFileName)
          .setFileFormat(FileFormat.BAI)
          .setFileSize(getFileSize(baiFile.get()))
          .setFileMd5sum(getChecksum(baiFile.get()));
    }

    analysisFile.addDonor()
        .setPrimarySite(context.getPrimarySite(projectCode))
        .setProjectCode(projectCode)
        .setProgram(project.getProgram())
        .setStudy(null) // Set downstream
        .setDonorId(context.getDonorId(legacyDonorId, projectCode))
        .setSpecimenId(context.getSpecimenId(legacySpecimenId, projectCode))
        .setSampleId(context.getSampleId(legacySampleId, projectCode))
        .setSubmittedDonorId(getParticipantId(result))
        .setSubmittedSpecimenId(getSampleId(result))
        .setSubmittedSampleId(getAliquotId(result))
        .setOtherIdentifiers(new OtherIdentifiers()
            .setTcgaParticipantBarcode(legacyDonorId)
            .setTcgaSampleBarcode(legacySpecimenId)
            .setTcgaAliquotBarcode(legacySampleId));

    return analysisFile;
  }

  //
  // Utilities
  //

  private Optional<JsonNode> resolveBaiFile(JsonNode result) {
    return resolveFiles(result, file -> isBaiFile(file)).findFirst();
  }

  private Stream<JsonNode> resolveFiles(JsonNode result, Predicate<? super JsonNode> filter) {
    return stream(getFiles(result)).filter(filter);
  }

  private static RepositoryProject resolveProject(JsonNode result) {
    val diseaseCode = getDiseaseAbbr(result);

    return getDiseaseCodeProject(diseaseCode).orNull();
  }

  private static String resolveDataType(JsonNode result) {
    val analyteCode = getAnalyteCode(result);
    if (DNA_SEQ_ANALYTE_CODES.contains(analyteCode)) {
      return DataType.ALIGNED_READS;
    } else if (RNA_SEQ_ANALYTE_CODES.contains(analyteCode)) {
      return DataType.RNA_SEQ;
    }

    return null;
  }

  private static long resolveLastModified(JsonNode result) {
    return Instant.parse(getLastModified(result)).getEpochSecond();
  }

  private static boolean isBamFile(JsonNode file) {
    return getFileName(file).endsWith(".bam");
  }

  private static boolean isBaiFile(JsonNode file) {
    return getFileName(file).endsWith(".bai");
  }

}