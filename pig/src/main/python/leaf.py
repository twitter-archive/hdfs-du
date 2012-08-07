#!/usr/bin/python

# Copyright 2012 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Mark paths with if they are leaf nodes or not.

import sys

fh = open(sys.argv[1])

prev = None

for line in fh:
  line = line.strip()
  if prev == None:
    prev = line
    continue

  if line.split('\t')[0].startswith(prev.split('\t')[0]):
    print "%s\t0" % prev
  else:
    print "%s\t1" % prev

  prev = line

print "%s\t1" % prev
