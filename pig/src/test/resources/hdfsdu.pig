-- Copyright 2012 Twitter, Inc.
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- pig -param INPUT=fsimage-delimited.tsv -param OUTPUT=hdfsdu.out /path/to/hdfsdu.pig

-- register pig/target/hdfsdu-pig-0.1.0.jar;

DEFINE extractsizes com.twitter.hdfsdu.pig.piggybank.ExtractSizes();

rmf $OUTPUT

data = LOAD '$INPUT' USING PigStorage('\t') AS (path:chararray,
                                                replication:int,
                                                modTime:chararray,
                                                accessTime:chararray,
                                                blockSize:long,
                                                numBlocks:int,
                                                fileSize:long,
                                                NamespaceQuota:int,
                                                DiskspaceQuota:int,
                                                perms:chararray,
                                                username:chararray,
                                                groupname:chararray);

size_by_path_data = foreach data generate path, fileSize;
B = FOREACH size_by_path_data GENERATE flatten(extractsizes(*)) as (path:chararray, size:long);
grouped_B = GROUP B BY path;
size_by_path = FOREACH grouped_B GENERATE group as path, SUM(B.size) as bytes;

count_by_path_data = foreach data generate path, 1L;
B = FOREACH count_by_path_data GENERATE flatten(extractsizes(*)) as (path:chararray, count:long);
grouped_B = GROUP B BY path;
count_by_path = FOREACH grouped_B GENERATE group as path, SUM(B.count) as count;

joined = join size_by_path by path, count_by_path by path;
final_output = foreach joined generate
  size_by_path::path as path,
  size_by_path::bytes as bytes,
  count_by_path::count as count;
final_output = ORDER final_output BY path;

store final_output into '$OUTPUT' using PigStorage('\t') parallel 1;
