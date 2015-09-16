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
package org.icgc.dcc.repository.pcawg.core;

import org.icgc.dcc.repository.core.model.RepositoryFile.AnalysisMethod;
import org.icgc.dcc.repository.core.model.RepositoryFile.AnalysisType;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataCategorization;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataType;
import org.icgc.dcc.repository.core.model.RepositoryFile.ExperimentalStrategy;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryFile.Software;
import org.icgc.dcc.repository.pcawg.model.Analysis;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PCAWGFileInfoResolver {

  public static AnalysisMethod resolveAnalysisMethod(@NonNull Analysis analysis) {
    val analysisMethod = new AnalysisMethod();
    if (analysis.isRNAAlignment()) {
      analysisMethod
          .setAnalysisType(AnalysisType.REFERENCE_ALIGNMENT)
          .setSoftware(analysis.getLibraryStrategyName());
    } else if (analysis.isBWAAlignment()) {
      analysisMethod
          .setAnalysisType(AnalysisType.REFERENCE_ALIGNMENT)
          .setSoftware(Software.BWA_MEM);
    } else if (analysis.isSangerVariantCalling()) {
      analysisMethod
          .setAnalysisType(AnalysisType.VARIANT_CALLING)
          .setSoftware(Software.SANGER_VAR);
    }

    return analysisMethod;
  }

  public static DataCategorization resolveDataCategorization(@NonNull Analysis analysis, @NonNull String fileName) {
    val category = new DataCategorization();

    if (analysis.isRNAAlignment()) {
      category
          .setDataType(DataType.ALIGNED_READS)
          .setExperimentalStrategy(ExperimentalStrategy.RNA_SEQ);
    } else if (analysis.isBWAAlignment()) {
      category
          .setDataType(DataType.ALIGNED_READS)
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    } else if (analysis.isSangerVariantCalling()) {
      category
          .setDataType(resolveSangerVariantCallingDataType(fileName))
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    }

    return category;
  }

  public static String resolveFileFormat(@NonNull Analysis analysis, @NonNull String fileName) {
    if (analysis.isRNAAlignment() || analysis.isBWAAlignment()) {
      val extension = ".bam";
      if (!fileName.endsWith(extension)) {
        log.warn("File with name '{}' and {} does not end in '{}'!", fileName, analysis, extension);
      }
      return FileFormat.BAM;
    } else if (analysis.isSangerVariantCalling()) {
      val dataType = resolveSangerVariantCallingDataType(fileName);
      return dataType == null ? null : FileFormat.VCF;
    } else {
      return null;
    }
  }

  private static String resolveSangerVariantCallingDataType(String fileName) {
    if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
      return DataType.SSM;
    } else if (fileName.endsWith(".somatic.cnv.vcf.gz")) {
      return DataType.CNSM;
    } else if (fileName.endsWith(".somatic.sv.vcf.gz")) {
      return DataType.STSM;
    } else if (fileName.endsWith(".somatic.indel.vcf.gz")) {
      return DataType.SSM;
    } else {
      return null;
    }
  }

}
