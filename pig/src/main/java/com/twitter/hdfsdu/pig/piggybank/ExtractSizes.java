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

package com.twitter.hdfsdu.pig.piggybank;

import java.io.File;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class ExtractSizes extends EvalFunc<DataBag> {
	private static BagFactory bf = BagFactory.getInstance();
	private static TupleFactory tf = TupleFactory.getInstance();
	PigStatusReporter reporter = PigStatusReporter.getInstance();

	private static enum counters {
		WRONG_INPUT
	};

	public DataBag exec(Tuple input) throws IOException {
		// verify input
		if (input == null) {
			if (reporter != null) {
				reporter.getCounter(counters.WRONG_INPUT).increment(1);
			}
			return null;
		}
		DataBag returnBag = bf.newDefaultBag();

		String path = (String) input.get(0);
		Long fileSize = (Long) input.get(1);

		File f = new File(path);
		while (f.getParent() != null) {
			String parent = f.getParent();
			Tuple t = tf.newTuple();
			t.append(parent);
			t.append(fileSize);
			returnBag.add(t);
			f = f.getParentFile();
		}
		return returnBag;
	}
}
