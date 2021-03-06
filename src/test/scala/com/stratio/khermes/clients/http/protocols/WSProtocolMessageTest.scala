/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.khermes.clients.http.protocols

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class WSProtocolMessageTest extends FlatSpec with Matchers  {

  "A WSProtocolCommand" should "parse a configuration" in {
    val block =
      """
        |[command]
        |ls
        |[arg1]
        |one line content
        |[arg2]
        |multiline line 1 content
        |multiline line 2 content
      """.stripMargin

    val result = WsProtocolCommand.parseTextBlock(block)
    result should be(WSProtocolMessage(WsProtocolCommand.Ls, Map(
      "arg1" -> "one line content",
      "arg2" -> "multiline line 1 content\nmultiline line 2 content"
    )))
  }
}
