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
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getRefassemShortName;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getResults;
import static org.icgc.dcc.repository.cghub.util.CGHubAnalysisDetails.getSampleId;
import static org.icgc.dcc.repository.cghub.util.CGHubConverters.convertAnalyteCode;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getDiseaseCodeProject;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getCGHubServer;

import java.time.Instant;
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

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CGHubFileProcessor extends RepositoryFileProcessor {

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

    return resolveIncludedFiles(result)
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
    val refAssembly = getRefassemShortName(result);
    val fileName = getFileName(file);
    val objectId = resolveObjectId(analysisId, fileName);

    //
    // Create
    //

    val analysisFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setObjectId(objectId)
        .setStudy(null) // N/A
        .setAccess(FileAccess.CONTROLLED);

    analysisFile.getDataBundle()
        .setDataBundleId(analysisId);

    analysisFile.getDataCategorization()
        .setDataType(resolveDataType(refAssembly))
        .setExperimentalStrategy(getLibraryStrategy(result));

    analysisFile.getAnalysisMethod()
        .setAnalysisType(resolveAnalysisType(refAssembly));

    analysisFile.getReferenceGenome()
        .setReferenceName(resolveReferenceName(refAssembly))
        .setGenomeBuild(resolveGenomeBuild(refAssembly));

    val fileCopy = analysisFile.addFileCopy()
        .setFileName(fileName)
        .setFileFormat(isBamFile(file) ? FileFormat.BAM : FileFormat.FASTQ)
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
      val baiObjectId = resolveObjectId(analysisId, baiFileName);
      fileCopy.getIndexFile()
          .setId(context.ensureFileId(baiObjectId))
          .setObjectId(baiObjectId)
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
        .setSpecimenType(resolveSpecimenType(result))
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

  private static Stream<JsonNode> resolveIncludedFiles(JsonNode result) {
    return resolveFiles(result, file -> isBamFile(file) || isFastqFile(file));
  }

  private static Optional<JsonNode> resolveBaiFile(JsonNode result) {
    return resolveFiles(result, file -> isBaiFile(file)).findFirst();
  }

  private static Stream<JsonNode> resolveFiles(JsonNode result, Predicate<? super JsonNode> filter) {
    return stream(getFiles(result)).filter(filter);
  }

  private static RepositoryProject resolveProject(JsonNode result) {
    val diseaseCode = getDiseaseAbbr(result);

    return getDiseaseCodeProject(diseaseCode).orNull();
  }

  private static long resolveLastModified(JsonNode result) {
    return Instant.parse(getLastModified(result)).getEpochSecond();
  }

  private static String resolveDataType(String refAssembly) {
    return "unaligned".equals(refAssembly) ? DataType.UNALIGNED_READS : DataType.ALIGNED_READS;
  }

  private static String resolveAnalysisType(String refAssembly) {
    return "unaligned".equals(refAssembly) ? null : "Reference alignment";
  }

  private static String resolveReferenceName(String refAssembly) {
    return "unaligned".equals(refAssembly) ? null : refAssembly;
  }

  private static String resolveGenomeBuild(String refAssembly) {
    return //
    "unaligned".equals(refAssembly) ? null : //
    refAssembly.startsWith("HG19") || refAssembly.startsWith("NCBI37") || refAssembly.startsWith("GRCh37") ? "GRCh37" : //
    refAssembly.startsWith("HG18") || refAssembly.startsWith("NCBI36") || refAssembly.startsWith("GRCh36") ? "GRCh36" : //
    null;
  }

  private static String resolveSpecimenType(JsonNode result) {
    val analyteCode = getAnalyteCode(result);
    return convertAnalyteCode(analyteCode);
  }

  private static boolean isBamFile(JsonNode file) {
    return hasFileExtension(file, ".bam");
  }

  private static boolean isBaiFile(JsonNode file) {
    return hasFileExtension(file, ".bai");
  }

  private static boolean isFastqFile(JsonNode file) {
    return hasFileExtension(file, "rnaseq_fastq.tar");
  }

  private static boolean hasFileExtension(JsonNode file, String fileType) {
    return getFileName(file).endsWith(fileType);
  }

}
