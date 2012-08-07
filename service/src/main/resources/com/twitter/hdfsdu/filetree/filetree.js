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

(function() {

function FileTree(chart_id) {

  var context = this;

  // misc varibles

  this.margin = {top: 0, right: 0, bottom: 0, left: 60};
  this.width = $(chart_id).width();
  this.height = $(chart_id).height();
  this.root;
  this.sibling_limit = 5;
  this.query_limit = 100000;
  this.duration = 500;
  this.letter_size = 8;
  this.column_width = 150;
  this.depth = 2;
  this.small_file_height = "0.9em"
  this.large_file_height = "3em"
  this.data_source = "/tree_size_by_path";

  // constants

  // construct tree

  this.tree = d3.layout.tree()
    .size([this.height, this.width]);

  this.tree.separation(function (a, b) {
    return a.parent == b.parent ? 1 : 1.5;
  });

  // create link connector factory

  this.diagonal = function(d) {
    var s = d.source;
    var t = d.target;

    if (s.name) {
      s = {x:s.x, y: s.y + context.letter_size * s.name.length};
    }

    var path = "M X0, Y0 C X1, Y1, X2, Y2, X3, Y3";
    var points = [];
    points.push({x: s.y, y: s.x});
    points.push({x: (s.y + t.y) / 2, y: s.x});
    points.push({x: (s.y + t.y) / 2, y: t.x});
    points.push({x: t.y, y: t.x});
    path = context.populate_path(path, points);
    return path;
  }


  // create the svg work space

  this.vis = d3.select(chart_id).append("svg")
    .attr("width", this.width)
    .attr("height", this.height)
    .append("g")
    .attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")");

}

FileTree.prototype.handle_data = function(json_string, open_root)
{
	var context = this,
		json = JSON.parse(json_string);

    context.root = json;
    context.preprocess_tree(context.root);
    context.root.x0 = context.height / 2;
    context.root.y0 = 0;

    // match (as closely as possible) the collapsed stat of the old tree

    function match_collapse(node) {

      if (node.children) {

        // recurse down through the nodes

        node.children.forEach(match_collapse);

        // close nodes by default

        context.close_node(node);

        // if the old version of the node is exists and is open, open this new version

        if (context.nodes) {
          context.nodes.forEach(function(old_node) {
            if (old_node.id == node.id && context.is_open(old_node))
              context.open_node(node);
          });
        }
      }
    }

    // match the collapsed state of the old tree

    match_collapse(context.root);

    // if there was no old tree, then open up the root node

    if (context.nodes === undefined || open_root)
      context.open_node(context.root);

    context.update(context.root);
};


FileTree.prototype.load_data = function(path, open_root)
{
	var that = this;
	new XHR({
		url: this.data_source,
		params: {
			path: path,
			depth: this.depth,
			limit: this.query_limit
		},
		onSuccess: function(text) {
			that.handle_data(text, open_root);
		}
	}).send();
}

FileTree.prototype.preprocess_tree = function(tree) {

  var context = this;

  // sort children

  tree.children.sort(function(a, b) {
    return b.data.fileSize - a.data.fileSize;
  });

  // collapse large lists

  if (tree.children.length > context.sibling_limit) {
    tree.other = tree.children.slice(context.sibling_limit);
    tree.children = tree.children.slice(0, context.sibling_limit);
    var other_sum = "0";
    var other_count = 0;
    tree.other.forEach(function (child) {
      var x = "-" + child.data.fileSize;
      other_sum = other_sum - x;
      ++other_count;
    });

    tree.children.push({
      name: other_count + " more...",
       id: tree.id + "/more...",
      data: {fileSize: other_sum},
      children: tree.other});
  }

  // process each child

  for (var i in tree.children) {
    context.preprocess_tree(tree.children[i]);
  }
}

FileTree.prototype.update = function(source) {

  var context = this;

  // compute the new tree layout

  context.nodes = this.tree.nodes(this.root).reverse();

  // normalize for fixed-depth and do any pre formatting

  context.nodes.forEach(function(d) {
    d.y = d.depth * context.column_width;
    context.preformat_node(d);
  });

  // bind nodes to the dom

  var node = this.vis.selectAll("g.node")
      .data(context.nodes, function(d) { return d.id || (d.id = d.id);});

  // enter  new nodes at the parent's previous position

  var enter_nodes = node
    .enter()
    .append("g")
    .attr("class", "node")
    .attr("transform", function(d) {
      var x = source.y0;
      var y = source.x0;

      // if (d.parent !== undefined) {
      //   x = d.parent.y;
      //   y = d.parent.x;
      // };

      return "translate(" + x + "," + y + ")";
    })
    .on("mouseover", function(d) { Observer.fireEvent('mouseover', d)})
    .on("mouseout", function(d) { Observer.fireEvent('mouseout', d);})
    .on("click", function(d) { Observer.fireEvent('click', d);});

  // add node circle

  enter_nodes
    .filter(function (d) {return !d.leaf;})
    .append("circle")
    .attr("class",  function(d) {return context.is_open(d) ? "open_branch" : "closed_branch"; })
    .attr("r", 1e-6);

  // add node text

  enter_nodes
    .append("svg:foreignObject")
    .classed("text_fo", true)
    .attr("width", "8em")
    .attr("height", context.small_file_height)
    .attr("x", ".5em")
    .attr("y", "-.3em")
    .append("xhtml:body")
    .attr("class", function(d) {return d.leaf ? "leaf_text" : "branch_text";})
    .html(context.node_html);

  d3.selectAll("text.arrow_text").
    remove();

  // add up arrow

  d3.selectAll("g.node")
    .filter(function(d) {
      return d.name != "/" && d.parent == null;})
    .append("svg:text")
    .attr("class", "arrow_text")
    .attr("x", -context.margin.left)
    .attr("dy", ".35em")
    .text(String.fromCharCode(0x2b05))
    .on("click", function(d) { return Observer.fireEvent('back', d);});



  // transition nodes to their new position.

  var update_nodes = node.transition()
    .duration(context.duration)
    .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

  // add node circle

  update_nodes.select("circle")
    .attr("class",  function(d) {return context.is_open(d) ? "open_branch" : "closed_branch"; })
    .attr("r", 6);
    // .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

  update_nodes.select("text")
      .style("fill-opacity", 1);

  // transition exiting nodes to the parent's new position.

  var exit_nodes = node.exit().transition()
      .duration(context.duration)
      .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
      .remove();

  exit_nodes.select("circle")
    .attr("r", 1e-6);

  exit_nodes.select("text")
      .style("fill-opacity", 1e-6);

  // bind links to dom

  var link = context.vis.selectAll("path.link")
      .data(context.tree.links(context.nodes), function(d) { return d.target.id; });

  // nnter any new links at the parent's previous position

  link.enter().insert("path", "g")
      .attr("class", "link")
      .attr("d", function(d) {
        var o = {x: source.x0, y: source.y0};
        return context.diagonal({source: o, target: o});
      });

  // transition links to their new position

  link.transition()
      .duration(context.duration)
      .attr("d", context.diagonal);

  // transition exiting nodes to the parent's new position

  link.exit().transition()
      .duration(context.duration)
      .attr("d", function(d) {
        var o = {x: source.x, y: source.y};
        return context.diagonal({source: o, target: o});
      })
      .remove();

  // stash the old positions for transition

  context.nodes.forEach(function(d) {
    d.x0 = d.x;
    d.y0 = d.y;
  });
}

FileTree.prototype.preformat_node = function(node) {
  node.size = this.format_number_string(node.data.fileSize);
}

FileTree.prototype.node_html = function(node) {
  var size_html = node.size !== undefined
    ? HT.tRow({}, HT.tCell({class: "size_text"}, "size: " + node.size)) : "";

  var html = HT.table({class: "node_text"}, HT.tRow({}, HT.tCell({}, node.name)) + size_html);

  return html;
};


FileTree.prototype.populate_path = function(path, points){
  for(index in points) {
    path = path
      .replace("X" + index, points[index].x)
      .replace("Y" + index, points[index].y);
  };
  return path;
}

// get node by id

FileTree.prototype.get_node_by_id = function(node_id) {
  var found = undefined;
  this.nodes.forEach(function (node) {
    if (node_id == node.id)
      found = node;
  });
  return found;
}


// handle up click

FileTree.prototype.up_click = function(node) {
  console.log("up click", node.name);
  var target = node.id.substring(0, node.id.lastIndexOf("/"));
  target = target.length == 0 ? "/" : target;
  this.load_data(target, true);
}

// handle node click

FileTree.prototype.click = function(node) {

	console.log('click', node.id);

  var id = node.id;
  for (var i = 0, l = this.nodes.length; i < l; ++i) {
	  if (id == this.nodes[i].id) {
		  node = this.nodes[i];
		  break;
	  }
  }

  if (i == l) {
	  console.log('did not find the node');
	  return;
  }

  console.log("expand click", node.name);

  // toggle children on this node

  this.toggle_children(node);

  // if this is a bottom most visible node, time to zoom into it

  if (node.depth == this.depth) {

    // pretend mouse left this node (cause it's gonna)

    this.mouseout(node);

    // establish node ancestor

    var ancestor = node.parent;
    while (ancestor.depth > 1)
      ancestor = ancestor.parent;

    // load ancestor

    this.load_data(ancestor.id, false);
  }

  // otherwise simply update this node

  else {
    this.update(node);
  }
}

// handle node hover

FileTree.prototype.mouseover = function(node) {
  var context = this;

  d3.selectAll("circle")
    .filter(function(d) {return d.id == node.id;})
    .classed("node_hover", true);

  d3.selectAll(".text_fo")
    .filter(function(d) {return d.id == node.id;})
    .attr("height", context.large_file_height);
}

FileTree.prototype.mouseout = function(node) {
  var context = this;

  d3.selectAll("circle")
    .filter(function(d) {return d.id == node.id})
    .classed("node_hover", false);

  d3.selectAll(".text_fo")
    .filter(function(d) {return d.id == node.id;})
    .attr("height", context.small_file_height);
}

// make node root

FileTree.prototype.make_root = function(node) {
  this.root = node;
}

// toggle children

FileTree.prototype.toggle_children = function(node) {
  this.is_open(node)
    ? this.close_node(node)
    : this.open_node(node);
}

FileTree.prototype.open_node = function(node) {
  node.children = node._children;
  node._children = null;
}

FileTree.prototype.close_node = function(node) {
  node._children = node.children;
  node.children = null;
}


FileTree.prototype.is_open = function(node) {
  return node.children != null;
}

FileTree.prototype.get_children = function(node) {
  return node.children ? node.children : node._children;
}

FileTree.prototype.is_branch = function(node) {
  var children = this.get_children(node);
  return children !== undefined && children != null && children.length > 0;
}

FileTree.prototype.show_node = function(node, indent) {
  indent = indent | 0;
  console.log(Array(indent).join(" ") + node.name + "(" + node.x + ", " + node.y + ")");
};


FileTree.prototype.format_number_string = function(number_string)
{
  var billions = number_string.length > 15;
  number_string = billions ? number_string.substring(0, number_string.length - 6) : number_string;
  return this.format_numbers(parseInt(number_string), billions);
}

FileTree.prototype.format_numbers = function(number, billions)
{
  var context = this;

  var digits = Math.log(number) / Math.log(10);

  // if the number is less then and not billions large, return return the nacked number

  if (number < 1000 && !billions)
    return number;

  for (var power = 3; power <= 18; power += 3) {
    var scale = Math.pow(10, power);
    if (number < scale)
      return context.format_number(number, Math.pow(10, power - 3), billions);
  }

  return "BIG!";
}

FileTree.prototype.format_number = function(number, scale, billions)
{
  var tags = {
    1000: "K",
    1000000: "M",
    1000000000: "G",
    1000000000000: "T",
    1000000000000000: "P"
  };

  var billions_tags = {
    1000: "G",
    1000000: "T",
    1000000000: "P",
    1000000000000: "E",
    1000000000000000: "Z"
  };

  var value = Math.round(10 * (number / scale)) / 10;
  var tag = billions ? billions_tags[scale] : tags[scale];
  return value+tag;
}

FileTree.prototype.test_format_numbers = function()
{
  var context = this;

  var test_numbers = [
    {input: "9", output: "9"},
    {input: "10", output: "10"},
    {input: "999", output: "999"},
    {input: "1100", output: "1.1K"},
    {input: "999900", output: "999.9K"},
    {input: "1100000", output: "1.1M"},
    {input: "999900000", output: "999.9M"},
    {input: "1100000000", output: "1.1G"},
    {input: "999900000000", output: "999.9G"},
    {input: "1100000000000", output: "1.1T"},
    {input: "999900000000000", output: "999.9T"},
    {input: "1100000000000000", output: "1.1P"},
    {input: "999900000000000000", output: "999.9P"},
    {input: "1100000000000000000", output: "1.1E"},
    {input: "999900000000000000000", output: "999.9E"},
    {input: "1100000000000000000000", output: "1.1Z"},
    {input: "999900000000000000000000", output: "999.9Z"},
  ];

  test_numbers.forEach(function(test_number) {
    var candiate = context.format_number_string(test_number.input);
    if (candiate != test_number.output)
      console.log(test_number, "!=", candiate);
    // else
    //   console.log(test_number, "==", candiate);
  });
}

var file_tree;

Observer.addEvent('load', function() {
	file_tree = new FileTree("#filetree");
});

Observer.addEvent('initdataloaded', function(data) {
	file_tree.handle_data(data);
});

Observer.addEvent('click', function(node) {
	file_tree.click(node);
});

Observer.addEvent('mouseover', function(node) {
	file_tree.mouseover(node);
});

Observer.addEvent('mouseout', function(node) {
	file_tree.mouseout(node);
});

Observer.addEvent('back', function(node) {
	file_tree.up_click(node);
});

})();
