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

import static com.google.common.collect.Lists.newArrayList;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAccess;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAnalysisId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAnalysisWorkflowType;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseProjectId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseProjectName;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCases;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataCategory;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataFormat;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataType;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getExperimentalStrategy;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileName;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileSize;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexDataFormat;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileName;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileSize;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFiles;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexMd5sum;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getMd5sum;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getUpdatedDatetime;
import static org.icgc.dcc.repository.gdc.util.GDCProjects.getProjectCode;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.ReferenceGenome;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.val;

/**
 * Maps GDC files to ICGC repository file model.
 * 
 * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440
 */
public class GDCFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final ReferenceGenome GDC_REFERENCE_GENOME = new ReferenceGenome()
      .setGenomeBuild("GRCh38.p0")
      .setReferenceName("GRCh38.d1.vd1");

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer gdcServer;

  public GDCFileProcessor(RepositoryFileContext context, @NonNull RepositoryServer gdcServer) {
    super(context);
    this.gdcServer = gdcServer;
  }

  public Stream<RepositoryFile> process(Stream<ObjectNode> files) {
    return files.map(this::createFile);
  }

  private RepositoryFile createFile(ObjectNode file) {
    val fileId = getFileId(file);
    val analysisId = getAnalysisId(file);
    val objectId = resolveObjectId(analysisId, fileId);

    val gdcFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setStudy(resolveStudies(file))
        .setObjectId(null); // N/A

    gdcFile.setAccess(getAccess(file));

    gdcFile.getAnalysisMethod()
        .setSoftware(null) // N/A
        .setAnalysisType(resolveAnalysisType(file));

    gdcFile.getDataCategorization()
        .setExperimentalStrategy(getExperimentalStrategy(file))
        .setDataType(resolveDataType(file));

    val dataBundleId = resolveDataBundleId(file);
    gdcFile.getDataBundle()
        .setDataBundleId(dataBundleId);

    gdcFile.setReferenceGenome(GDC_REFERENCE_GENOME);

    val fileCopy = gdcFile.addFileCopy()
        .setRepoDataBundleId(dataBundleId)
        .setRepoFileId(fileId)
        .setRepoDataSetId(null) // N/A
        .setFileFormat(getDataFormat(file))
        .setFileSize(getFileSize(file))
        .setFileName(getFileName(file))
        .setFileMd5sum(getMd5sum(file))
        .setLastModified(resolveLastModified(file))
        .setRepoType(gdcServer.getType().getId())
        .setRepoOrg(gdcServer.getSource().getId())
        .setRepoName(gdcServer.getName())
        .setRepoCode(gdcServer.getCode())
        .setRepoCountry(gdcServer.getCountry())
        .setRepoBaseUrl(gdcServer.getBaseUrl())
        .setRepoMetadataPath(gdcServer.getType().getMetadataPath())
        .setRepoDataPath(gdcServer.getType().getDataPath());

    for (val indexFile : getIndexFiles(file)) {
      val indexFileId = getIndexFileId(indexFile);
      val indexObjectId = resolveObjectId(analysisId, indexFileId);
      fileCopy.getIndexFile()
          .setId(context.ensureFileId(indexObjectId))
          .setObjectId(null) // N/A
          .setRepoFileId(indexFileId)
          .setFileName(getIndexFileName(indexFile))
          .setFileFormat(getIndexDataFormat(indexFile))
          .setFileSize(getIndexFileSize(indexFile))
          .setFileMd5sum(getIndexMd5sum(indexFile));
    }

    for (val caze : getCases(file)) {
      gdcFile.addDonor()
          .setDonorId(getCaseId(caze)) // Set this here for now as it is needed by the combiner
          .setSubmittedDonorId(getCaseId(caze))
          .setProjectCode(resolveProjectCode(caze));
    }

    return gdcFile;
  }

  private static List<String> resolveStudies(@NonNull ObjectNode file) {
    val values = Sets.<String> newLinkedHashSet();
    for (val caze : getCases(file)) {
      values.add(getCaseProjectName(caze));
    }

    return newArrayList(values);
  }

  private static String resolveAnalysisType(@NonNull ObjectNode file) {
    return getAnalysisWorkflowType(file);
  }

  private static String resolveDataBundleId(@NonNull ObjectNode file) {
    return getAnalysisId(file);
  }

  private static String resolveDataType(@NonNull ObjectNode file) {
    return getDataCategory(file) + " " + getDataType(file);
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

}
