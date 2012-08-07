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

package com.twitter.hdfsdu.data;

import org.json.simple.JSONObject;

public class NodeData {
	public String path;
	public String fileSize;
	public Long nChildren = 0L;
  public boolean leaf;
	
	public JSONObject toJSON() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("path", this.path);
		jsonObject.put("fileSize", fileSize);
		jsonObject.put("nChildren", nChildren);
    jsonObject.put("leaf", leaf);
		return jsonObject;
	}
}
