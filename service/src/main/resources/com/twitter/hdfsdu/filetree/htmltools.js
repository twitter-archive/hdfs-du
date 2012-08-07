// Copyright 2012 Twitter, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var HT = {};

HT.input = function(attributes, text) {return HT.html("input", attributes, text);}
HT.form = function(attributes, text) {return HT.html("form", attributes, text);}
HT.table = function(attributes, text) {return HT.html("table", attributes, text);}
HT.tRow = function(attributes, text) {return HT.html("tr", attributes, text);}
HT.tCell = function(attributes, text) {return HT.html("td", attributes, text);}
HT.div = function(attributes, text) {return HT.html("div", attributes, text);}
HT.span = function(attributes, text) {return HT.html("span", attributes, text);}
HT.text = function(attributes, text) {return HT.html("text", attributes, text);}
HT.htmlP = function(attributes, text) {return HT.html("p", attributes, text);}
HT.htmlImg = function(attributes, source, altText)
{
  attributes = typeof attributes !== 'undefined' ? attributes : {};
  attributes = HT.mergeProperties({src: source, alt: altText}, attributes);
  return HT.html("img", attributes, "");
}
HT.htmlA = function(attributes, text, link)
{
  attributes = typeof attributes !== 'undefined' ? attributes : {};
  attributes = HT.mergeProperties({href: link}, attributes);
  return HT.html("a", attributes, text);
}

HT.svg = function(attributes, content)
{
  attributes = typeof attributes !== 'undefined' ? attributes : {};
  attributes = HT.mergeProperties({xmlns: "http://www.w3.org/2000/svg"}, attributes);
  return HT.html("svg", attributes, content);
}

HT.html = function(tag, attributes, content)
{
  attributes = typeof attributes !== 'undefined' ? attributes : {};
  var result = [];
  result.push('<' + tag);
  for (var attribute in attributes)
    result.push(' ' + attribute + '="' + attributes[attribute] + '"');
  result.push('>' + content + '</' + tag + '>');
  return result.join('');
}

HT.mergeProperties = function(obj1,obj2)
{
  var obj3 = {};
  for (var attrname in obj1) { obj3[attrname] = obj1[attrname]; }
  for (var attrname in obj2) { obj3[attrname] = obj2[attrname]; }
  return obj3;
}
