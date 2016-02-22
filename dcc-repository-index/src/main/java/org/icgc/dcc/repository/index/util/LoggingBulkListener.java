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
package org.icgc.dcc.repository.index.util;

import static com.google.common.base.Preconditions.checkState;

import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeValue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingBulkListener implements Listener {

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    log.info("[{}] executing [{}]/[{}]", executionId, request.numberOfActions(),
        new ByteSizeValue(request.estimatedSizeInBytes()));
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    log.info("'{}' executed  [{}]/[{}], took {}", executionId, request.numberOfActions(), new ByteSizeValue(
        request.estimatedSizeInBytes()), response.getTook());

    checkState(!response.hasFailures(), "'%s' failed to execute bulk request: %s", executionId,
        response.buildFailureMessage());
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable e) {
    log.info("'{}' failed to execute bulk request", e, executionId);
  }

}