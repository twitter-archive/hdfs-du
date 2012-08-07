/* 
 * Copyright 2012 Twitter, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.hdfsdu;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import org.hsqldb.Server;

import java.util.logging.Logger;

public class Database {
  private static final Logger LOG = Logger.getLogger(Database.class.getName());

  public static final String DB_NAME = "hdfsdu_test";

  @CmdLine(name = "db_port", help = "Path to the input data set.")
  private static final Arg<Integer> DB_PORT = Arg.create(14001);

  public Database() {
    LOG.info("Starting DB on port " + DB_PORT.get());
    Server db = new Server();
    db.setAddress("localhost");
    db.setPort(DB_PORT.get());
    db.setDatabasePath(0, "mem:" + DB_NAME + ";sql.enforce_strict_size=true;");
    db.setDatabaseName(0, DB_NAME);
    db.start();
    LOG.info("Started DB.");
  }
}
