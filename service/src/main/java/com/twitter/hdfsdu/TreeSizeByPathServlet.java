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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONObject;

import com.google.common.collect.Lists;
import com.twitter.hdfsdu.data.DataTransformer;
import com.twitter.hdfsdu.data.NodeData;

public class TreeSizeByPathServlet extends SizeByPathServlet {
	@Override
	public Iterable<String> getLines(HttpServletRequest request) {
		String paramPath = request.getParameter("path");
		if (paramPath == null) {
			paramPath = "/";
		}
		List<String> lines = Lists.newLinkedList();
		List<NodeData> elems = Lists.newArrayList();
		Integer paramDepth = request.getParameter("depth") == null ? 2
				: Integer.parseInt(request.getParameter("depth"));

    try {
			ResultSet resultSet = getSizeByPath(request);
			NodeData data;
			while (resultSet.next()) {
				data = new NodeData();
        data.fileSize = resultSet.getString("size_in_bytes");
        data.nChildren = resultSet.getLong("file_count");
        data.path = resultSet.getString("path");
        data.leaf = resultSet.getBoolean("leaf");
				elems.add(data);
			}
			JSONObject jsonObject = DataTransformer.getJSONTree(paramPath, paramDepth, elems);
			
			String ans = null;
			if (jsonObject != null) {
			 ans = jsonObject.toJSONString();
			}
			
			if (ans == null) {
				lines.add("{ \"children\": [] }");
			} else {
				lines.add(ans);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return lines;
	}
}
