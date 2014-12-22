/**
 * Created by mnunberg on 12/21/14.
 */

/**
 * This file contains the routines needed to properly generate a views index.
 */
var console = {
  log: function(s) {
    java.lang.System.err.println(s);
  },
  logf: function() {
    java.lang.System.err.printf.apply(java.lang.System.err, arguments);
  }
};

function Row(key, value, docid) {
  this.key = key;
  this.value = value;
  this.id = docid;

  if (value === undefined) {
    this.value = null;
  }
  if (key === undefined) {
    this.key = null;
  }
}

function DocRows(docMeta) {
  this.rows = [];
  this.rev = docMeta.rev;
}

function Index() {
  this.byId = {}; // docid => DocRows
  this.sortedAscending = [];
  this.sortedDescending = [];
  this.prev = undefined;
  this.sortOk = false;
  this.currentMeta = null;
}

//noinspection JSUnusedGlobalSymbols
Index.prototype.emit = function(key, value) {
  var id = this.currentMeta.id;
  var row = new Row(key, value, id);
  var docRows = this.byId[id];

  if (docRows === undefined) {
    docRows = new DocRows(this.currentMeta);
    this.byId[id] = docRows;
  }

  docRows.rows.push(row);
  this.sortOk = false; // Invalidate sort order!
};

Index.prototype.preSort = function() {
  var results_arr = [];
  for (var id in this.byId) {
    if (!this.byId.hasOwnProperty(id)) {
      continue;
    }
    var rowColl = this.byId[id].rows;

    for (var i = 0; i < rowColl.length; i++) {
      results_arr.push(rowColl[i]);
    }
  }

  this.sortedAscending = results_arr.sort(cbSortRow);
  this.sortedDescending = results_arr.slice(0).reverse();
  this.sortOk = true;
};

/**
 * Prepare the object to iterate over a set of potentially new items
 * @param {Index} prev the previous index
 */
Index.prototype.prepare = function() {
  var prevIndex = new Index();
  prevIndex.byId = this.byId;
  prevIndex.sortedAscending = this.sortedAscending;
  prevIndex.sortedDescending = this.sortedDescending;

  this.prev = prevIndex;
  this.byId = {};
  this.sortedAscending = [];
  this.sortedDescending = [];
  this.sortOk = true;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Indicate that the current generation has been completed.
 */
Index.prototype.setDone = function() {
  if (this.sortOk) {
    // Check to see we don't have any deleted items
    for (var id in this.prev.byId) {
      if (!this.prev.byId.hasOwnProperty(id)) {
        continue;
      }
      if (this.byId[id] === undefined) {
        // Found deleted element!
        this.sortOk = false;
        break;
      }
    }
  }

  if (this.sortOk) {
    //console.log("Using pre-sorted index");
    this.sortedAscending = this.prev.sortedAscending;
    this.sortedDescending = this.prev.sortedDescending;
  } else {
    //console.log("SortOK=false");
    this.preSort();
  }
};

//noinspection JSUnusedGlobalSymbols
Index.prototype.indexDoc = function(item, mapFunc) {
  // Check if UTF8
  var metaArg = {
    id: item.getKeySpec().key + "",
    rev: item.getCas()
  };

  var lastDocInfo = this.prev.byId[metaArg.id];
  if (lastDocInfo !== undefined && lastDocInfo.rev === metaArg.rev) {
    this.byId[metaArg.id] = this.prev.byId[metaArg.id];
    return; // Sort order preserved
  }

  var docArg;
  try {
    // See if we can convert to utf8
    docArg = JSON.parse(item.getUtf8());
    metaArg.type = "json";
  } catch (e) {
    docArg = item.getBase64();
    metaArg.type = "base64";
  }

  try {
    this.currentMeta = metaArg;
    mapFunc(docArg, metaArg);
  } catch (e) {
    console.log(e);
  }
};