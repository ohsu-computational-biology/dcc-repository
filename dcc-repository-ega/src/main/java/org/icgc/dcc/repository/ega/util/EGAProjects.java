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
package org.icgc.dcc.repository.ega.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

@NoArgsConstructor(access = PRIVATE)
public final class EGAProjects {

  private static final List<Record> DATASETS = ImmutableList.<Record> builder()
      .add(new Record("BLCA-CN", "EGAS00001000677", "EGAD00001000758"))
      .add(new Record("COCA-CN", "EGAS00001001088", "Unknown"))
      .add(new Record("COCA-CN", "EGAS00001001200", "Unknown"))
      .add(new Record("COCA-CN", "EGAS00001001309", "Unknown"))
      .add(new Record("COCA-CN", "EGAS00001001310", "Unknown"))
      .add(new Record("ESCA-CN", "EGAS00001000709", "EGAD00001000760"))
      .add(new Record("ESCA-CN", "EGAS00001001475", "Unknown"))
      .add(new Record("ESCA-CN", "EGAS00001001518", "Unknown"))
      .add(new Record("GACA-CN", "EGAS00001000675", "Unknown"))
      .add(new Record("LUSC-CN", "EGAS00001001087", "Unknown"))
      .add(new Record("RECA-CN", "EGAS00001000676", "Unknown"))
      .add(new Record("LIAD-FR", "EGAS00001000679", "EGAD00001000737"))
      .add(new Record("LICA-FR", "EGAS00001000217", "EGAD00001000131"))
      .add(new Record("LICA-FR", "EGAS00001000679", "EGAD00001000737"))
      .add(new Record("LICA-FR", "EGAS00001000706", "EGAD00001000749"))
      .add(new Record("LICA-FR", "EGAS00001001002", "EGAD00001001096"))
      .add(new Record("LIHM-FR", "EGAS00001001002", "EGAD00001001096"))
      .add(new Record("BOCA-FR", "EGAS00001000855", "EGAD00001001051"))
      .add(new Record("BOCA-UK", "EGAS00001000038", "EGAD00001000358"))
      .add(new Record("BOCA-UK", "EGAS00001000038", "EGAD00010000432"))
      .add(new Record("BRCA-UK", "EGAS00001000206", "EGAD00001000133"))
      .add(new Record("BRCA-UK", "EGAS00001000031", "EGAD00001000138"))
      .add(new Record("CMDI-UK", "EGAS00001000089", "EGAD00001000045"))
      .add(new Record("CMDI-UK", "EGAS00001000089", "EGAD00001000117"))
      .add(new Record("CMDI-UK", "EGAS00001000089", "EGAD00001000283"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00001000263"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00001000689"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00001000891"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00001000892"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00001001116"))
      .add(new Record("PRAD-UK", "EGAS00001000262", "EGAD00010000498"))
      .add(new Record("ESAD-UK", "EGAS00001000559", "EGAD00001000704"))
      .add(new Record("ESAD-UK", "EGAS00001000559", "EGAD00001001071"))
      .add(new Record("ESAD-UK", "EGAS00001000724", "EGAD00001001048"))
      .add(new Record("ESAD-UK", "EGAS00001000724", "EGAD00001001067"))
      .add(new Record("ESAD-UK", "EGAS00001000724", "EGAD00001001071"))
      .add(new Record("ESAD-UK", "EGAS00001000724", "EGAD00001001394"))
      .add(new Record("ESAD-UK", "EGAS00001000724", "EGAD00001001457"))
      .add(new Record("ESAD-UK", "EGAS00001000725", "Unknown"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00001000023"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00001000044"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00001000083"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00010000238"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00010000280"))
      .add(new Record("CLLE-ES", "EGAS00000000092", "EGAD00010000470"))
      .add(new Record("CLLE-ES", "EGAS00001000374", "EGAD00010000472"))
      .add(new Record("CLLE-ES", "Unknown", "EGAD00010000642"))
      .add(new Record("CLLE-ES", "EGAS00001001306", "EGAD00010000805"))
      .add(new Record("CLLE-ES", "EGAS00001000374", "EGAD00001000258"))
      .add(new Record("CLLE-ES", "EGAS00001001306", "EGAD00001001443"))
      .add(new Record("CLLE-ES", "EGAS00001000272", "EGAD00001000177"))
      .add(new Record("CLLE-ES", "EGAS00001000272", "EGAD00010000254"))
      .add(new Record("EOPC-DE", "EGAS00001000400", "EGAD00001000303"))
      .add(new Record("EOPC-DE", "EGAS00001000400", "EGAD00001000304"))
      .add(new Record("EOPC-DE", "EGAS00001000400", "EGAD00001000305"))
      .add(new Record("EOPC-DE", "EGAS00001000400", "EGAD00001000306"))
      .add(new Record("EOPC-DE", "EGAS00001000400", "EGAD00001000632"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000278"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000279"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000281"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000355"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000356"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000645"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000648"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00001000650"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00010000377"))
      .add(new Record("MALY-DE", "EGAS00001000394", "EGAD00010000379"))
      .add(new Record("MALY-DE", "EGAS00001001067", "EGAD00001001119"))
      .add(new Record("MALY-DE", "EGAS00001001067", "EGAD00001001120"))
      .add(new Record("MALY-DE", "EGAS00001001067", "EGAD00001001121"))
      .add(new Record("PBCA-DE", "EGAS00001000085", "EGAD00001000027"))
      .add(new Record("PBCA-DE", "EGAS00001000085", "EGAD00001000697"))
      .add(new Record("PBCA-DE", "EGAS00001000215", "EGAD00001000122"))
      .add(new Record("PBCA-DE", "EGAS00001000215", "EGAD00001000327"))
      .add(new Record("PBCA-DE", "EGAS00001000215", "EGAD00001000328"))
      .add(new Record("PBCA-DE", "EGAS00001000215", "EGAD00001000697"))
      .add(new Record("PBCA-DE", "EGAS00001000381", "EGAD00001000271"))
      .add(new Record("PBCA-DE", "EGAS00001000381", "EGAD00001000616"))
      .add(new Record("PBCA-DE", "EGAS00001000381", "EGAD00001000617"))
      .add(new Record("PBCA-DE", "EGAS00001000393", "EGAD00001000275"))
      .add(new Record("PBCA-DE", "EGAS00001000607", "EGAD00001000697"))
      .add(new Record("PBCA-DE", "EGAS00001000607", "EGAD00001000698"))
      .add(new Record("PBCA-DE", "EGAS00001000607", "EGAD00001000699"))
      .add(new Record("PBCA-DE", "EGAS00001000744", "EGAD00001000816"))
      .add(new Record("PBCA-DE", "EGAS00001000561", "EGAD00001000644"))
      .add(new Record("PBCA-DE", "EGAS00001000561", "EGAD00010000562"))
      .add(new Record("LINC-JP", "EGAS00001000389", "EGAD00001000446"))
      .add(new Record("LINC-JP", "EGAS00001000389", "EGAD00001001024"))
      .add(new Record("LINC-JP", "EGAS00001000389", "EGAD00001001030"))
      .add(new Record("LINC-JP", "EGAS00001000389", "EGAD00001001270"))
      .add(new Record("LINC-JP", "EGAS00001000671", "EGAD00001001262"))
      .add(new Record("LINC-JP", "EGAS00001000671", "EGAD00001001263"))
      .add(new Record("LIRI-JP", "EGAS00001000678", "EGAD00001000808"))
      .add(new Record("BTCA-JP", "EGAS00001000950", "EGAD00001001076"))
      .add(new Record("ORCA-IN", "EGAS00001000249", "EGAD00001000272"))
      .add(new Record("ORCA-IN", "EGAS00001001028", "EGAD00001001060"))
      .add(new Record("MELA-AU", "EGAS00001001552", "Unknown"))
      .add(new Record("OV-AU", "EGAS00001000154", "EGAD00001000049"))
      .add(new Record("OV-AU", "EGAS00001000154", "EGAD00001000096"))
      .add(new Record("OV-AU", "EGAS00001000154", "EGAD00001000323"))
      .add(new Record("OV-AU", "EGAS00001000154", "EGAD00001000660"))
      .add(new Record("OV-AU", "EGAS00001000154", "EGAD00001000371"))
      .add(new Record("PACA-AU", "EGAS00001000154", "EGAD00001000049"))
      .add(new Record("PACA-AU", "EGAS00001000154", "EGAD00001000096"))
      .add(new Record("PACA-AU", "EGAS00001000154", "EGAD00001000323"))
      .add(new Record("PACA-AU", "EGAS00001000154", "EGAD00001000660"))
      .add(new Record("PACA-AU", "EGAS00001000154", "EGAD00001000371"))
      .add(new Record("PAEN-AU", "EGAS00001000154", "EGAD00001000049"))
      .add(new Record("PAEN-AU", "EGAS00001000154", "EGAD00001000096"))
      .add(new Record("PAEN-AU", "EGAS00001000154", "EGAD00001000323"))
      .add(new Record("PAEN-AU", "EGAS00001000154", "EGAD00001000660"))
      .add(new Record("PAEN-AU", "EGAS00001000154", "EGAD00001000371"))
      .add(new Record("PACA-CA", "EGAS00001000395", "EGAD00001001595"))
      .add(new Record("PACA-CA", "EGAS00001000395", "EGAD00001001095"))
      .add(new Record("PRAD-CA", "EGAS00001000900", "EGAD00001001094"))
      .add(new Record("RECA-EU", "EGAS00001000083", "EGAD00001000718"))
      .add(new Record("RECA-EU", "EGAS00001000083", "EGAD00001000719"))
      .add(new Record("RECA-EU", "EGAS00001000083", "EGAD00001000709"))
      .add(new Record("RECA-EU", "EGAS00001000083", "EGAD00001000717"))
      .add(new Record("RECA-EU", "EGAS00001000083", "EGAD00001000720"))
      .add(new Record("PAEN-IT", "EGAS00001000154", "EGAD00001000049"))
      .add(new Record("PAEN-IT", "EGAS00001000154", "EGAD00001000096"))
      .add(new Record("PAEN-IT", "EGAS00001000154", "EGAD00001000323"))
      .add(new Record("PAEN-IT", "EGAS00001000154", "EGAD00001000660"))
      .add(new Record("PAEN-IT", "EGAS00001000154", "EGAD00001000371"))
      .add(new Record("THCA-SA", "EGAS00001000680", "Unknown"))
      .add(new Record("SKCA-BR", "EGAS00001001052", "Unknown"))
      .add(new Record("LAML-KR", "EGAS00001001082", "Unknown"))
      .add(new Record("LAML-KR", "EGAS00001001559", "Unknown"))
      .add(new Record("LUSC-KR", "EGAS00001001083", "Unknown"))
      .build();

  @Value
  private static class Record {

    String projectCode;
    String studyId;
    String datasetId;

  }

  public static List<String> getStudyProjectCodes(@NonNull String studyId) {
    return DATASETS.stream()
        .filter(r -> r.getStudyId().equals(studyId))
        .map(Record::getProjectCode)
        .collect(toImmutableList());
  }

  public static List<String> getDatasetProjectCodes(@NonNull String datasetId) {
    return DATASETS.stream()
        .filter(r -> r.getDatasetId().equals(datasetId))
        .map(Record::getProjectCode)
        .collect(toImmutableList());
  }

}
