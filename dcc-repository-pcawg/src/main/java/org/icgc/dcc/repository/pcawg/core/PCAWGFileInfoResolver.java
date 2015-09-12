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

import org.icgc.dcc.repository.core.model.RepositoryFile.DataCategorization;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataType;
import org.icgc.dcc.repository.core.model.RepositoryFile.ExperimentalStrategy;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PCAWGFileInfoResolver {

  public DataCategorization resolveDataCategorization(@NonNull String analysisType, @NonNull String fileName) {
    // TODO: Talk to JJ on these ifs. May need to adjust logic
    val category = new DataCategorization();

    if (isRNASeq(analysisType)) {
      category
          .setDataType(DataType.RNA_SEQ)
          .setExperimentalStrategy(ExperimentalStrategy.RNA_SEQ);
    } else if (isDNASeq(analysisType)) {
      category
          .setDataType(DataType.ALIGNED_READS)
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    } else if (isSangerVariantCalling(analysisType)) {
      category
          .setDataType(resolveSangerVariantCallingDataType(fileName))
          .setExperimentalStrategy(ExperimentalStrategy.WGS);
    }

    return category;
  }

  public String resolveFileFormat(@NonNull String analysisType, @NonNull String fileName) {
    if (isRNASeq(analysisType) || isDNASeq(analysisType)) {
      // TODO: Verify with JJ that this should be BAM for DNA-Seq
      return FileFormat.BAM;
    } else if (isSangerVariantCalling(analysisType)) {
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

  private static boolean isRNASeq(String analysisType) {
    return analysisType.matches("rna_seq\\..*\\.(star|tophat)");
  }

  private static boolean isDNASeq(String analysisType) {
    return analysisType.matches("wgs\\..*\\.bwa_alignment");
  }

  private static boolean isSangerVariantCalling(String analysisType) {
    return analysisType.matches("wgs\\.tumor_specimens\\.sanger_variant_calling");
  }

}
