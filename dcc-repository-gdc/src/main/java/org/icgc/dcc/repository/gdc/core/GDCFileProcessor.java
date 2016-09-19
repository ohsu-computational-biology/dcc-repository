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
package org.icgc.dcc.repository.gdc.core;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAccess;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAliquotId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAliquotSubmitterId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAnalysisId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAnalysisWorkflowType;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getAnalyteAliquots;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseProjectId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseProjectPrimarySite;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseSampleId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseSampleType;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCaseSamples;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getCases;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getDataCategory;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getDataFormat;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getDataType;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getExperimentalStrategy;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getFileId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getFileName;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getFileSize;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexDataFormat;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexFileId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexFileName;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexFileSize;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexFiles;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getIndexMd5sum;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getMd5sum;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getPortionAnalytes;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getSamplePortions;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getSampleSubmitterId;
import static org.icgc.dcc.common.gdc.core.GDCFiles.getUpdatedDatetime;
import static org.icgc.dcc.common.gdc.core.GDCProjects.getProjectCode;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectByProjectCode;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryFile.ReferenceGenome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps GDC files to ICGC repository file model.
 * 
 * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440
 */
@Slf4j
public class GDCFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final String SEQUENCING_STRATEGY_CODE_LIST_NAME = "GLOBAL.0.sequencing_strategy.v1";
  private static final String EXCLUDED_EXPERIMENTAL_STRATEGY = "non-NGS";

  /**
   * Metadata.
   */
  @NonNull
  private final Repository gdcRepository;
  private final Set<String> experimentalStrategies;

  /**
   * State.
   */
  private int fileCount = 0;

  public GDCFileProcessor(RepositoryFileContext context, @NonNull Repository gdcRepository) {
    super(context);
    this.gdcRepository = gdcRepository;
    this.experimentalStrategies = resolveExperimentalStrategies();
  }

  public Stream<RepositoryFile> process(Stream<ObjectNode> files) {
    return files.map(this::createFile).filter(this::isIncluded);
  }

  private RepositoryFile createFile(ObjectNode file) {
    val dataType = resolveDataType(file);
    if (dataType == null) {
      // Ignored
      return null;
    }

    val fileId = getFileId(file);
    val objectId = resolveObjectId(fileId);

    val gdcFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setStudy(null) // N/A
        .setObjectId(objectId);

    gdcFile.setAccess(getAccess(file));

    gdcFile.getAnalysisMethod()
        .setSoftware(resolveAnalysisSoftware(file))
        .setAnalysisType(null); // N/A

    gdcFile.getDataCategorization()
        .setExperimentalStrategy(resolveExperimentalStrategy(file, experimentalStrategies))
        .setDataType(dataType);

    val dataBundleId = resolveDataBundleId(file);
    gdcFile.getDataBundle()
        .setDataBundleId(dataBundleId);

    gdcFile.setReferenceGenome(ReferenceGenome.GDC);

    val fileCopy = gdcFile.addFileCopy()
        .setRepoDataBundleId(dataBundleId)
        .setRepoFileId(fileId)
        .setRepoDataSetId(null) // N/A
        .setFileFormat(getDataFormat(file))
        .setFileSize(getFileSize(file))
        .setFileName(getFileName(file))
        .setFileMd5sum(getMd5sum(file))
        .setLastModified(resolveLastModified(file))
        .setRepoType(gdcRepository.getType().getId())
        .setRepoOrg(gdcRepository.getSource().getId())
        .setRepoName(gdcRepository.getName())
        .setRepoCode(gdcRepository.getCode())
        .setRepoCountry(gdcRepository.getCountry())
        .setRepoBaseUrl(gdcRepository.getBaseUrl())
        .setRepoMetadataPath(gdcRepository.getType().getMetadataPath())
        .setRepoDataPath(gdcRepository.getType().getDataPath());

    for (val indexFile : getIndexFiles(file)) {
      val indexFileId = getIndexFileId(indexFile);
      val indexObjectId = resolveObjectId(indexFileId);
      fileCopy.getIndexFile()
          .setId(context.ensureFileId(indexObjectId))
          .setObjectId(indexObjectId)
          .setRepoFileId(indexFileId)
          .setFileName(getIndexFileName(indexFile))
          .setFileFormat(getIndexDataFormat(indexFile))
          .setFileSize(getIndexFileSize(indexFile))
          .setFileMd5sum(getIndexMd5sum(indexFile));

      // There should only be one
      break;
    }

    for (val caze : getCases(file)) {
      val projectCode = resolveProjectCode(caze);
      val project = getProjectByProjectCode(projectCode).orNull();
      val submittedDonorId = resolveSubmittedDonorId(caze);

      // Only include donors that are found in DCC
      if (!context.isDCCSubmittedDonorId(projectCode, submittedDonorId)) {
        continue;
      }

      gdcFile.addDonor()
          .setPrimarySite(resolvePrimarySite(caze, projectCode))
          .setProgram(project.getProgram())
          .setProjectCode(projectCode)
          .setStudy(null) // N/A
          .setDonorId(null) // Set downstream
          .setSpecimenId(null) // Set downstream
          .setSpecimenType(resolveSpecimenType(caze, dataType))
          .setMatchedControlSampleId(resolveMatchedControlSampleId(caze, dataType))
          .setSampleId(null) // Set downstream
          .setSubmittedDonorId(submittedDonorId)
          .setSubmittedSpecimenId(resolveSubmitterSpecimenId(caze, dataType))
          .setSubmittedSampleId(resolveSubmitterSampleId(caze, dataType))
          .setOtherIdentifiers(new OtherIdentifiers()
              .setTcgaParticipantBarcode(resolveTcgaParticipantBarcode(caze))
              .setTcgaSampleBarcode(resolveTcgaSampleBarcode(caze, dataType))
              .setTcgaAliquotBarcode(resolveTcgaAliquotBarcode(caze, dataType)));

    }

    // Only include files that are on donors found in DCC
    if (gdcFile.getDonors().isEmpty()) {
      return null;
    }

    // "Downstream"
    assignStudy(singleton(gdcFile));
    assignIds(singleton(gdcFile));

    if (++fileCount % 1000 == 0) {
      log.info("Processed {} files", formatCount(fileCount));
    }

    return gdcFile;
  }

  private boolean isIncluded(RepositoryFile file) {
    if (file == null) {
      return false;
    }

    // JJ: Experimental strategy must be matched to one of ICGC's 'sequencing_strategy' that is not Non-NGS
    if (EXCLUDED_EXPERIMENTAL_STRATEGY.equals(file.getDataCategorization().getExperimentalStrategy())) {
      return false;
    }

    return true;
  }

  private Set<String> resolveExperimentalStrategies() {
    val codeList = findCodeList(SEQUENCING_STRATEGY_CODE_LIST_NAME).get();

    val values = ImmutableSet.<String> builder();
    for (val term : codeList.path("terms")) {
      values.add(term.get("value").textValue());
    }

    return values.build();
  }

  private String resolvePrimarySite(JsonNode caze, String projectCode) {
    val primarySite = context.getPrimarySite(projectCode);
    if (primarySite != null) {
      return primarySite;
    }

    return getCaseProjectPrimarySite(caze);
  }

  private static String resolveAnalysisSoftware(@NonNull ObjectNode file) {
    return getAnalysisWorkflowType(file);
  }

  private static String resolveExperimentalStrategy(ObjectNode file, Set<String> values) {
    val experimentalStrategy = getExperimentalStrategy(file);
    if (experimentalStrategy == null) {
      return null;
    }

    for (val value : values) {
      if (experimentalStrategy.equalsIgnoreCase(value)) {
        return value;
      }
    }

    return EXCLUDED_EXPERIMENTAL_STRATEGY;
  }

  private static String resolveDataBundleId(@NonNull ObjectNode file) {
    return getAnalysisId(file);
  }

  /**
   * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440#
   * ICGCrepositorymetadataJSONmodelupdatetoaccommodateEGA/GDCintegration-Datacategory/typemapping
   */
  private static String resolveDataType(@NonNull ObjectNode file) {
    // Inputs
    val dataCategory = getDataCategory(file);
    val dataType = getDataType(file);

    // Special cases
    final String ignored = null, unexpected = null;

    // Output
    switch (dataCategory) {

    case "Raw Sequencing Data":
      switch (dataType) {
      case "Aligned Reads":
        return dataType;
      case "Aligned Reads Index":
      case "Experiment Metadata":
      case "Run Metadata":
      case "Analysis Metadata":
        return ignored;
      default:
        return unexpected;
      }

    case "Simple Nucleotide Variation":
      switch (dataType) {
      case "Simple Somatic Mutation":
      case "Aggregated Somatic Mutations":
      case "Annotated Somatic Mutation":
        return "SSM";
      case "Simple Germline Variation":
        return "SGV";
      case "Raw Simple Somatic Mutation":
        return ignored;
      default:
        return unexpected;
      }

    case "Copy Number Variation":
      switch (dataType) {
      case "Copy Number Somatic Variation":
        return "CNSM";
      case "Copy Number Germline Variation":
        return "CNGV";
      default:
        return unexpected;
      }

    case "Structural Rearrangement":
      switch (dataType) {
      case "Structural Somatic Rearrangement":
        return "StSM";
      case "Structural Germline Rearrangement":
        return "StGV";
      default:
        return unexpected;
      }

    case "Clinical":
      switch (dataType) {
      case "Clinical Supplement":
        return "Clinical Data";
      case "Pathology Report":
        return "Pathology Report";
      default:
        return unexpected;
      }
    case "Biospecimen":
      switch (dataType) {
      case "Biospecimen Supplement":
        return "Biospecimen Data";
      case "Slide Image":
        return "Slide Image";
      default:
        return unexpected;
      }
    case "Transcriptome Profiling":
    case "DNA Methylation":
    case "Protein Expression":
      return ignored;

    default:
      return unexpected;
    }
  }

  private static Long resolveLastModified(@NonNull ObjectNode file) {
    val text = getUpdatedDatetime(file);
    val temporal = ISO_OFFSET_DATE_TIME.parse(text);

    return Instant.from(temporal).getEpochSecond();
  }

  private static String resolveProjectCode(@NonNull JsonNode caze) {
    val projectId = getCaseProjectId(caze);
    return getProjectCode(projectId);
  }

  private static String resolveSubmittedDonorId(JsonNode caze) {
    return getCaseId(caze);
  }

  private static List<String> resolveSubmitterSpecimenId(JsonNode caze, String dataType) {
    val submitterSpecimenId = Lists.<String> newArrayList();
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType)) {
        submitterSpecimenId.add(getCaseSampleId(sample));
      }
    }

    return submitterSpecimenId;
  }

  private static List<String> resolveSpecimenType(JsonNode caze, String dataType) {
    val specimenType = Lists.<String> newArrayList();
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType)) {
        specimenType.add(sampleType);
      }
    }

    return specimenType;
  }

  private static String resolveMatchedControlSampleId(JsonNode caze, String dataType) {
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType) == false) {
        for (val portion : getSamplePortions(sample)) {
          for (val analyte : getPortionAnalytes(portion)) {
            for (val aliquot : getAnalyteAliquots(analyte)) {
              // JJ: at the inner most level, the aliquot should be just one so return first one should be safe. this is
              // because, one VCF can only associate with one tumour aliquot (or one normal aliquot)

              // This is a barcode so that we can convert it to an ICGC id "Downstream"
              return getAliquotSubmitterId(aliquot);
            }
          }
        }
      }
    }

    return null;
  }

  private static List<String> resolveSubmitterSampleId(JsonNode caze, String dataType) {
    val submitterSampleId = Lists.<String> newArrayList();
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType)) {
        for (val portion : getSamplePortions(sample)) {
          for (val analyte : getPortionAnalytes(portion)) {
            for (val aliquot : getAnalyteAliquots(analyte)) {
              submitterSampleId.add(getAliquotId(aliquot));
            }
          }
        }
      }
    }

    return submitterSampleId;
  }

  private static String resolveTcgaParticipantBarcode(JsonNode caze) {
    return getAliquotSubmitterId(caze);
  }

  private static List<String> resolveTcgaSampleBarcode(JsonNode caze, String dataType) {
    val tcgaSampleBarcode = Lists.<String> newArrayList();
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType)) {
        tcgaSampleBarcode.add(getSampleSubmitterId(sample));
      }
    }

    return tcgaSampleBarcode;
  }

  private static List<String> resolveTcgaAliquotBarcode(JsonNode caze, String dataType) {
    val tcgaAliquotBarcode = Lists.<String> newArrayList();
    for (val sample : getCaseSamples(caze)) {
      val sampleType = getCaseSampleType(sample);

      if (isPrimaryBiospecimen(dataType, sampleType)) {
        for (val portion : getSamplePortions(sample)) {
          for (val analyte : getPortionAnalytes(portion)) {
            for (val aliquot : getAnalyteAliquots(analyte)) {
              tcgaAliquotBarcode.add(getAliquotSubmitterId(aliquot));
            }
          }
        }
      }
    }

    return tcgaAliquotBarcode;
  }

  private static boolean isPrimaryBiospecimen(String dataType, String sampleType) {
    val matched = dataType.toLowerCase().endsWith("variation") || dataType.toLowerCase().endsWith("mutation");
    if (!matched) {
      return true;
    }

    val control = sampleType.toLowerCase().contains("normal");
    if (!control) {
      return true;
    }

    return false;
  }

}
