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
package org.icgc.dcc.repository.pcawg.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Longs.max;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.Repositories.getPCAWGRepository;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.repository.pcawg.core.PCAWGFileInfoResolver.resolveAnalysisMethod;
import static org.icgc.dcc.repository.pcawg.core.PCAWGFileInfoResolver.resolveDataCategorization;
import static org.icgc.dcc.repository.pcawg.core.PCAWGFileInfoResolver.resolveFileFormat;
import static org.icgc.dcc.repository.pcawg.model.Analysis.analysis;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGIES;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileMd5sum;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileMd5sum;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFiles;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosLastModified;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSpecimenType;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryFile.ReferenceGenome;
import org.icgc.dcc.repository.core.model.RepositoryFile.Study;
import org.icgc.dcc.repository.pcawg.model.Analysis;
import org.icgc.dcc.repository.pcawg.model.Workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PCAWGFileProcessor extends RepositoryFileProcessor {

  public PCAWGFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processDonors(@NonNull Iterable<ObjectNode> donors) {
    log.info("Creating donor files...");
    val donorFiles = createDonorFiles(donors);

    log.info("Translating TCGC UUIDs...");
    translateTCGAUUIDs(donorFiles);

    log.info("Assigning ICGC IDs...");
    assignIds(donorFiles);

    return donorFiles;
  }

  private Iterable<RepositoryFile> createDonorFiles(Iterable<ObjectNode> donors) {
    return stream(donors).map(this::processDonor).flatMap(List::stream).collect(toImmutableList());
  }

  private List<RepositoryFile> processDonor(@NonNull ObjectNode donor) {
    val donorFiles = ImmutableList.<RepositoryFile> builder();
    for (val libraryStrategy : PCAWG_LIBRARY_STRATEGIES) {
      for (val specimenClass : PCAWG_SPECIMEN_CLASSES) {
        for (val specimen : resolveSpecimens(donor, libraryStrategy, specimenClass)) {
          for (val workflow : resolveWorkflows(libraryStrategy, specimenClass, specimen))
            for (val workflowFile : resolveWorkflowFiles(workflow)) {
              donorFiles.add(
                  createDonorFile(
                      getDccProjectCode(donor),
                      getSubmitterDonorId(donor),
                      workflow.getAnalysis(),
                      workflow.getWorkflow(),
                      workflowFile));
            }
        }
      }
    }

    return donorFiles.build();

  }

  private RepositoryFile createDonorFile(String projectCode, String submittedDonorId, Analysis analysis,
      JsonNode workflow, JsonNode workflowFile) {

    //
    // Prepare
    //

    val project = getProjectCodeProject(projectCode).orNull();
    checkState(project != null, "No project found for project code '%s'", projectCode);

    val gnosId = getGnosId(workflow);
    val specimenType = getSpecimenType(workflow);
    val submitterSpecimenId = getSubmitterSpecimenId(workflow);
    val submitterSampleId = getSubmitterSampleId(workflow);

    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);
    val fileFormat = resolveFileFormat(analysis, fileName);
    val objectId = resolveObjectId(gnosId, fileName);

    val pcawgRepositories = resolvePCAWGRepositories(workflow);

    val baiFile = resolveBaiFile(workflow, fileName);
    val tbiFile = resolveTbiFile(workflow, fileName);

    if (baiFile.isPresent() && tbiFile.isPresent()) {
      log.warn("Both '{}' and '{}' files are present at the same time!", baiFile.get(), tbiFile.get());
    }

    //
    // Create
    //

    val donorFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setObjectId(objectId)
        .setStudy(ImmutableList.of(Study.PCAWG))
        .setAccess(FileAccess.CONTROLLED);

    donorFile.getDataBundle()
        .setDataBundleId(gnosId);

    donorFile
        .setAnalysisMethod(resolveAnalysisMethod(analysis));

    donorFile
        .setDataCategorization(resolveDataCategorization(analysis, fileName));

    donorFile
        .setReferenceGenome(ReferenceGenome.PCAWG);

    for (val pcawgRepository : pcawgRepositories) {
      val fileCopy = donorFile.addFileCopy()
          .setFileName(fileName)
          .setFileFormat(fileFormat)
          .setFileSize(fileSize)
          .setFileMd5sum(resolveMd5sum(workflowFile))
          .setLastModified(resolveLastModified(workflow))
          .setRepoDataBundleId(gnosId)
          .setRepoFileId(null) // GNOS does not have individual file ids
          .setRepoType(pcawgRepository.getType().getId())
          .setRepoOrg(pcawgRepository.getSource().getId())
          .setRepoName(pcawgRepository.getName())
          .setRepoCode(pcawgRepository.getCode())
          .setRepoCountry(pcawgRepository.getCountry())
          .setRepoBaseUrl(pcawgRepository.getBaseUrl())
          .setRepoDataPath(pcawgRepository.getType().getDataPath())
          .setRepoMetadataPath(pcawgRepository.getType().getMetadataPath());

      if (baiFile.isPresent()) {
        val baiFileName = getFileName(baiFile.get());
        val baiObjectId = resolveObjectId(gnosId, baiFileName);
        fileCopy.getIndexFile()
            .setId(context.ensureFileId(baiObjectId))
            .setObjectId(baiObjectId)
            .setRepoFileId(null) // TODO: Resolve
            .setFileName(baiFileName)
            .setFileFormat(FileFormat.BAI)
            .setFileSize(getFileSize(baiFile.get()))
            .setFileMd5sum(resolveMd5sum(baiFile.get()));
      }
      if (tbiFile.isPresent()) {
        val tbiFileName = getFileName(tbiFile.get());
        val tbiObjectId = resolveObjectId(gnosId, tbiFileName);
        fileCopy.getIndexFile()
            .setId(context.ensureFileId(tbiObjectId))
            .setObjectId(tbiObjectId)
            .setFileName(tbiFileName)
            .setFileFormat(FileFormat.TBI)
            .setFileSize(getFileSize(tbiFile.get()))
            .setFileMd5sum(resolveMd5sum(tbiFile.get()));
      }
    }

    donorFile.addDonor()
        .setPrimarySite(context.getPrimarySite(projectCode))
        .setProgram(project.getProgram())
        .setProjectCode(projectCode)
        .setStudy(Study.PCAWG)
        .setDonorId(null) // Set downstream
        .setSpecimenId(null) // Set downstream
        .setSpecimenType(singletonList(specimenType))
        .setSampleId(null) // Set downstream
        .setSubmittedDonorId(submittedDonorId)
        .setSubmittedSpecimenId(singletonList(submitterSpecimenId))
        .setSubmittedSampleId(singletonList(submitterSampleId))
        .setOtherIdentifiers(new OtherIdentifiers()
            .setTcgaParticipantBarcode(null) // Set downstream
            .setTcgaSampleBarcode(null) // Set downstream
            .setTcgaAliquotBarcode(null)); // Set downstream

    return donorFile;
  }

  //
  // Utilities
  //

  private static Iterable<JsonNode> resolveSpecimens(ObjectNode donor, String libraryStrategy, String specimenClass) {
    val specimens = donor.path(libraryStrategy).path(specimenClass);
    return specimens.isArray() ? specimens : singleton(specimens);
  }

  private static List<Workflow> resolveWorkflows(String libraryStrategy, String specimenClass, JsonNode specimen) {
    val workflows = ImmutableList.<Workflow> builder();
    for (val workflowType : resolveWorkflowTypes(specimen)) {
      val analysis = analysis()
          .libraryStrategy(libraryStrategy)
          .specimenClass(specimenClass)
          .workflowType(workflowType).build();

      workflows.add(new Workflow(analysis, specimen.get(workflowType)));
    }

    return workflows.build();
  }

  private static Iterable<String> resolveWorkflowTypes(JsonNode specimen) {
    return () -> specimen.fieldNames();
  }

  private static List<JsonNode> resolveWorkflowFiles(Workflow workflow) {
    return resolveFiles(workflow.getWorkflow(), workflowFile -> !isExcluded(workflow.getAnalysis(), workflowFile))
        .collect(toImmutableList());
  }

  private static Optional<JsonNode> resolveBaiFile(JsonNode workflow, String fileName) {
    val baiFileName = fileName + ".bai";
    return resolveFiles(workflow, file -> baiFileName.equals(resolveFileName(file))).findFirst();
  }

  private static Optional<JsonNode> resolveTbiFile(JsonNode workflow, String fileName) {
    val tbiFileName = fileName + ".tbi";
    return resolveFiles(workflow, file -> tbiFileName.equals(resolveFileName(file))).findFirst();
  }

  private static Stream<JsonNode> resolveFiles(JsonNode workflow, Predicate<? super JsonNode> filter) {
    return stream(getFiles(workflow)).filter(filter);
  }

  private static List<Repository> resolvePCAWGRepositories(JsonNode workflow) {
    return stream(getGnosRepo(workflow))
        .map(genosRepo -> getPCAWGRepository(genosRepo.asText()))
        .collect(toImmutableList());
  }

  private static String resolveFileName(JsonNode workflowFile) {
    return firstNonNull(getBamFileName(workflowFile), getFileName(workflowFile));
  }

  private static String resolveMd5sum(JsonNode workflowFile) {
    return firstNonNull(getBamFileMd5sum(workflowFile), getFileMd5sum(workflowFile));
  }

  private static long resolveFileSize(JsonNode workflowFile) {
    // First non-zero
    return max(getFileSize(workflowFile), getBamFileSize(workflowFile));
  }

  private static long resolveLastModified(JsonNode workflow) {
    val lastModified = getGnosLastModified(workflow);
    checkState(lastModified != null, "Date is null for workflow: %s", workflow);
    val dateTime = ISO_OFFSET_DATE_TIME.parse(lastModified, Instant::from);

    return dateTime.getEpochSecond();
  }

  /**
   * @see https://wiki.oicr.on.ca/display/DCCSOFT/Uniform+metadata+JSON+document+for+ICGC+Data+Repositories#
   * UniformmetadataJSONdocumentforICGCDataRepositories-Datatypeassignmentforvariantcallresultfiles
   */
  private static boolean isExcluded(Analysis analysis, JsonNode workflowFile) {
    val excluded = true;
    val included = false;
    val fileName = getFileName(workflowFile);

    if (!isBamFile(workflowFile) && !isVcfFile(workflowFile) && !isXmlFile(workflowFile)) {
      return excluded;
    }
    if (fileName.contains(".germline.") &&
        analysis.getWorkflowType().equals("sanger_variant_calling")) {
      return excluded;
    }
    if (analysis.getLibraryStrategy().equals("wgs") &&
        analysis.getSpecimenClass().equals("tumor_specimens") &&
        analysis.getWorkflowType().equals("broad_tar_variant_calling")) {
      return excluded;
    }
    if (analysis.getWorkflowType().equals("minibam")) {
      // JJ: "There is no rush for that, I heard miniBAMs may need to be regenerated."
      return excluded;
    }
    if (resolveDataCategorization(analysis, fileName).getDataType() == null) {
      // JJ: Ignore files like these for now (e.g. *.embl-delly_1-0-0-preFilter-hpc.*):
      // 712ba532-fb1a-43fa-a356-b446b509ceb7.embl-delly_1-0-0-preFilter-hpc.150708.sv.vcf.gz
      return excluded;
    }

    return included;
  }

  private static boolean isBamFile(JsonNode file) {
    return hasFileExtension(file, ".bam");
  }

  private static boolean isVcfFile(JsonNode file) {
    return hasFileExtension(file, ".vcf.gz");
  }

  private static boolean isXmlFile(JsonNode file) {
    return hasFileExtension(file, ".xml");
  }

  private static boolean hasFileExtension(JsonNode file, String fileType) {
    return getFileName(file).toLowerCase().endsWith(fileType.toLowerCase());
  }

}
