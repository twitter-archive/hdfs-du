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

(function() {
	var QueryConfig = {
		limit: 50000,
		depth: 2
	};
	
	var Observer = {
		callbacks: {},
		addEvent: function(type, fn) {
			var callbacks = this.callbacks;
			if (!callbacks[type]) {
				callbacks[type] = [];
			}
			callbacks[type].push(fn);
		},
		
		fireEvent: function(type, e) {
			var callbacks = this.callbacks[type];
			if (!callbacks) return;
			callbacks.forEach(function(cb) {
				cb(e);
			});
		}
	};
	
	var XHR = function(opt) {
		var xhr = new XMLHttpRequest(),
			qs = opt.params || {},
			url = opt.url;
			
		var k = Object.keys(qs),
			queryString = [];
		k.forEach(function(key) {
			queryString.push(key + '=' + encodeURIComponent(qs[key]));
		});
		
		if (queryString.length) {
			queryString = queryString.join('&');
			queryString = '?' + queryString;
			url += queryString;
		}
		
		xhr.open('GET', url, true);
		xhr.onreadystatechange = function(e) {
			if (xhr.readyState == 4) {
				opt.onSuccess && opt.onSuccess(xhr.responseText);
			}
		};
		
		this.xhr = xhr;
	};

	XHR.prototype = {
		send: function() {
			this.xhr.send(null);
		}
	};
	window.Observer = Observer;
	window.XHR = XHR;
	
	window.addEventListener('DOMContentLoaded', function(e) {
		Observer.fireEvent('load', e);
		
		new XHR({
			url: '/tree_size_by_path',
			params: {
				path: '/',
				limit: QueryConfig.limit,
				depth: QueryConfig.depth
			},
			onSuccess: function(text) {
				Observer.fireEvent('initdataloaded', text);
			}
		}).send();	
	});
})();
