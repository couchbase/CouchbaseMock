/**
 * This file contains common sorting/comparison routines for views
 */

// http://docs.couchdb.org/en/latest/couchapp/views/collation.html
var SORT_ORDER = function() {
  var ordered_array = [
    'null',
    'false',
    'true',
    'number',
    'string',
    'array',
    'object',
    'unknown'
  ];

  var obj = {};
  for (var i = 0; i < ordered_array.length; i++) {
    obj[ordered_array[i]] = i;
  }
  return obj;
}();

/**
 * Returns the sorting priority for a given type
 * @param v The value whose type should be evaluated
 * @return The numeric sorting index
 */
function getSortIndex(v) {
  if (v === null) {
    return SORT_ORDER['null'];
  } else if (typeof v === 'string') {
    return SORT_ORDER['string'];
  } else if (typeof v === 'number') {
    return SORT_ORDER['number'];
  } else if (Array.isArray(v)) {
    return SORT_ORDER['array'];
  } else if (v === true) {
    return SORT_ORDER['true'];
  } else if (v === false) {
    return SORT_ORDER['false'];
  } else if (typeof v === 'object') {
    return SORT_ORDER['object'];
  } else {
    return SORT_ORDER['unknown'];
  }
}

/**
 * Compares one value with another
 * @param a The first value
 * @param b The second value
 * @param [exact] If both @c b and @c b are arrays, setting this parameter to true
 * ensures that they will only be equal if their length matches and their
 * contents match. If this value is false (the default), then only the common
 * subset of elements are evaluated
 *
 * @return {number} greater than 0 if @c a is bigger than @b; a number less
 * than 0 if @a is less than @b, or 0 if they are equal
 */
function cbCompare(a, b, exact) {
  if (Array.isArray(a) && Array.isArray(b)) {
    if (exact === true) {
      if (a.length !== b.length) {
        return a.length > b.length ? +1 : -1;
      }
    }
    var maxLength = a.length > b.length ? b.length : a.length;
    for (var i = 0; i < maxLength; ++i) {
      var subCmp = cbCompare(a[i], b[i], true);
      if (subCmp !== 0) {
        return subCmp;
      }
    }
    return 0;
  }

  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }

  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  // Now we need to do special things
  var aPriority = getSortIndex(a);
  var bPriority = getSortIndex(b);
  if (aPriority !== bPriority) {
    return aPriority - bPriority;
  } else {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return 1;
    } else {
      return 0;
    }
  }
}

/**
 * Find the index of @c val in the array @arr
 * @param arr The array to search in
 * @param val The value to search for
 * @return {number} the index in the array, or -1 if the item does not exist
 */
function cbIndexOf(arr, val) {
  for (var i = 0; i < arr.length; ++i) {
    //console.log("Comparing :" + JSON.stringify(val) + " with " + JSON.stringify(arr[i]));
    if (cbCompare(arr[i], val, true) === 0) {
      return i;
    }
  }
  return -1;
}

/**
 * Normalize a key for reduce
 * @param key The key to normalize
 * @param groupLevel The group level
 * @return {*}
 */
function cbNormKey(key, groupLevel) {
  if (groupLevel === 0) {
    return null;
  }

  if (Array.isArray(key)) {
    if (groupLevel === -1) {
      return key;
    } else {
      return key.slice(0, groupLevel);
    }
  } else {
    return key;
  }
}

/**
 * Compare two emitted rows against each other.
 * @param {Row} a
 * @param {Row} b
 * @return {number}
 */
function cbSortRow(a, b) {
  var ret = cbCompare(a.key, b.key);
  if (ret === 0) {
    return cbCompare(a.id, b.id);
  } else {
    return ret;
  }
}
