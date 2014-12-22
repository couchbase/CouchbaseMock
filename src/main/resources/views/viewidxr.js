'use strict';

// Disable dot-notation warnings in this file since we handle a lot
//   of random incoming view options, some that can't be dot-referenced.
/* jshint -W069 */

var console = {
  log: function(s) {
    java.lang.System.err.println(s);
  },
  logf: function() {
    java.lang.System.err.printf.apply(java.lang.System.err, arguments);
  }
};


function KeyFilter(fkeys) {
  this.keys_arrays = [];
  this.string_keys = {};
  this.num_keys = {};
  this.arr_1key_string = {};
  this.arr_1key_num = {};

  function insertQuick(key, numTarget, stringTarget) {
    if (typeof key === 'number') {
      numTarget[parseInt(key)] = true;
    } else {
      stringTarget[key] = true;
    }
  }

  function isQuick(key) {
    return typeof key === 'number' || typeof key === 'string';
  }

  function checkQuick(key, numTarget, stringTarget) {
    if (typeof key === 'string') {
      return key in stringTarget;
    } else {
      return parseInt(key) in numTarget;
    }
  }

  for (var i = 0; i < fkeys.length; i++) {
    var cKey = fkeys[i];
    if (isQuick(cKey)) {
      insertQuick(cKey, this.num_keys, this.string_keys);
    } else if (Array.isArray(cKey) && cKey.length === 1 && isQuick(cKey[0])) {
      insertQuick(cKey[0], this.arr_1key_num, this.arr_1key_string);
    } else {
      this.keys_arrays.push(cKey);
    }
  }

  this.exists = function(key) {
    if (isQuick(key)) {
      return checkQuick(key, this.num_keys, this.string_keys);
    } else if (Array.isArray(key) && key.length === 1 && isQuick(key[0])) {
      return checkQuick(key[0], this.arr_1key_num, this.arr_1key_string);
    } else {
      return cbIndexOf(this.keys_arrays, key) !== -1;
    }
  }
}

function _getKeyFilter(options) {
  if (options.keys !== undefined) {
    return new KeyFilter(options.keys);
  } else if (options.key) {
    return new KeyFilter([options.key]);
  } else {
    return undefined;
  }
}

function OptionProcessor(optmap) {
  this.error = undefined;
  this.optmap = optmap;
}

var OPTION_SPECS = {
  startkey:           { type: 'json' },
  endkey:             { type: 'json' },
  startkey_docid:     { type: 'raw' },
  endkey_docid:       { type: 'raw' },
  inclusive_start:    { type: 'boolean', default: true },
  inclusive_end:      { type: 'boolean', default: false },
  descending:         { type: 'boolean', default: false },
  key:                { type: 'json' },
  keys:               { type: 'object' },
  reduce:             { type: 'boolean', default: undefined },
  group:              { type: 'boolean', default: undefined },
  group_level:        { type: 'number', default: undefined },
  stale:              { type: 'raw', default: 'ok' },
  skip:               { type: 'number', default: undefined },
  limit:              { type: 'number', default: undefined },
  debug:              { type: 'boolean', default: false }
};


function badParam(reason) {
  throw {
    error: 'query_parse_error',
    reason: reason
  };
}

OptionProcessor.prototype.parse = function() {
  for (var optName in OPTION_SPECS) {
    var spec = OPTION_SPECS[optName];
    var value = this.optmap[optName];

    if (value === undefined) {
      this[optName] = spec.default;
    } else {
      if (spec.type === 'json') {
        this[optName] = JSON.parse(value);
      } else if (spec.type === 'raw') {
        this[optName] = value;
      } else {
        value = JSON.parse(value);
        if (typeof value !== spec.type) {
          badParam('invalid value for ' + spec.type + ' parameter: "' + value);
        }
        this[optName] = value;
      }
    }
  }
};

function execute(inputOptions, indexedItems, reduceFunc) {
  //console.log('indexer opts', options);
  var options = new OptionProcessor(inputOptions);
  options.parse();

  var startKey = options.startkey;
  var endKey = options.endkey;
  var startKeyDocId = options.startkey_docid;
  var endKeyDocId = options.endkey_docid;
  var inclusiveStart = options.inclusive_start;
  var inclusiveEnd = options.inclusive_end;
  var descending = options.descending;
  var doReduce = options.reduce;

  var filterKeys = _getKeyFilter(options);
  var results = [];

  if (descending) {
    var tmp;
    tmp = startKey; startKey = endKey; endKey = tmp;
    tmp = startKeyDocId; startKeyDocId = endKeyDocId; endKeyDocId = tmp;
    tmp = inclusiveStart; inclusiveStart = inclusiveEnd; inclusiveEnd = tmp;
  }

  // Validate that the startkey and endkey are ok
  if (startKey && endKey && cbCompare(startKey, endKey) > 0) {
    badParam('No rows can match your key range, reverse your start_key and end_key or set descending=false');
  }
  if (doReduce === undefined) {
    doReduce = reduceFunc !== undefined;
  } else if (doReduce && reduceFunc === undefined) {
    badParam('Invalid URL parameter `reduce` for map view.');
  }
  if (options.group_level !== undefined && options.group !== undefined) {
    badParam('Query parameter `group_level` is not compatible with `group`')
  }
  if (options.key !== undefined && options.keys !== undefined) {
    badParam('`keys` and `key` are incompatible. Specify one or the other');
  }

  var iterable;
  if (descending === true) {
    iterable = indexedItems.sortedDescending;
  } else {
    iterable = indexedItems.sortedAscending;
  }

  //console.log("Ascending: " + JSON.stringify(indexedItems.sortedAscending));
  //console.log("Descending: " + JSON.stringify(indexedItems.sortedDescending));
  //console.log("Iterating...");

  for (var i = 0; i < iterable.length; i++) {
    var row = iterable[i];
    var docKey = row.key;
    var docId = row.id;

    if (filterKeys !== undefined) {
      if (!filterKeys.exists(docKey)) {
        continue;
      }
    }

    var ret;
    if (startKey) {
      ret = cbCompare(docKey, startKey);
      if (ret === 0 && startKeyDocId) {
        ret = cbCompare(docId, startKeyDocId);
      }
      if (ret < 0 || (!inclusiveStart && ret < 1)) {
        continue;
      }
    }

    if (endKey) {
      ret = cbCompare(docKey, endKey);
      if (ret === 0 && endKeyDocId) {
        ret = cbCompare(docId, endKeyDocId);
      }
      if (ret > 0 || (!inclusiveEnd && ret >= 0)) {
        continue;
      }
    }
    results.push(row);
  }
  //console.log("Iteration done!");

  var groupLevel = options.group_level;
  if (groupLevel === undefined) {
    if (options.group === true) {
      groupLevel = -1;
    } else if (options.group === undefined) {
      groupLevel = 0;
    }
  }

  //console.log('REDUCER GROUPLEVEL: '+ groupLevel);
  if (doReduce) {
    //console.log('VIEW PRE REDUCE', results);

    var keysN = [];
    for (var m = 0; m < results.length; ++m) {
      var keyN = cbNormKey(results[m].key, groupLevel);
      if (cbIndexOf(keysN, keyN) < 0) {
        //console.log("Key " + JSON.stringify(keyN) + " Not yet in reduced items list!");
        keysN.push(keyN);
      }
    }

    var reducedResults = [];

    for (var j = 0; j < keysN.length; ++j) {
      var values = [];
      for (var k = 0; k < results.length; ++k) {
        var keyZ = cbNormKey(results[k].key, groupLevel);
        if (cbCompare(keyZ, keysN[j]) === 0) {
          values.push(results[k].value);
        }
      }
      var result = reduceFunc(keysN[j], values, false);
      reducedResults.push({
        key: keysN[j],
        value: result
      });
    }
    results = reducedResults;
  }

  /* FINALIZE */
  if (options.skip !== undefined) {
    results = results.slice(options.skip);
  }
  if (options.limit !== undefined) {
    results = results.slice(0, options.limit);
  }

  var retVal = {};
  retVal.rows = results;
  retVal.total_rows = iterable.length;

  if (options.debug) {
    retVal.debug_info = { "move along": ["nothing", "to", "see", "here"] };
  }

  //console.log(JSON.stringify(retVal));
  return retVal;
}
