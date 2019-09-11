/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import static org.testng.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

class TestUtils {
  static void checkCmd(String failureMsg, String... args) throws IOException, InterruptedException {
    Process cmd = new ProcessBuilder(args).redirectErrorStream(true).start();
    cmd.waitFor();
    if (cmd.exitValue() != 0) {
      BufferedReader br = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
      fail(failureMsg + "\n" + br.lines().collect(Collectors.joining("\n")));
    }
  }
}
