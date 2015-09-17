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
package org.icgc.dcc.repository.client.core;

import org.icgc.dcc.common.core.mail.Mailer;
import org.icgc.dcc.repository.client.core.RepositoryImporter.ReportMessage;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import lombok.val;

@Ignore("For development only")
public class ReportMessageTest {

  @Test
  public void testSuccessMessage() {
    val watch = Stopwatch.createStarted();
    val exceptions = ImmutableList.<Exception> of();
    val message = new ReportMessage(watch, exceptions);

    send(message);
  }

  @Test
  public void testErrorMessage() {
    val watch = Stopwatch.createStarted();
    val exceptions = ImmutableList.<Exception> of(
        new RuntimeException("This is message 1"),
        new RuntimeException("This is message 2"));
    val message = new ReportMessage(watch, exceptions);

    send(message);
  }

  private void send(ReportMessage message) {
    val userName = System.getProperty("user.name");
    val email = userName + "@oicr.on.ca";

    val mailer = Mailer.builder().enabled(true).recipient(email).build();
    mailer.sendMail(message.getSubject(), message.getBody());
  }

}
