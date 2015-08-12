/**
 * Created by mnunberg on 12/21/14.
 */


// Builtin functions:


var BUILTIN_REDUCERS = {
  '_count': function(key, values, rereduce) {
    if (rereduce) {
      var result = 0;
      for (var i = 0; i < values.length; i++) {
        result += values[i];
      }
      return result;
    } else {
      return values.length;
    }
  },
  '_sum': function(key, values, rereduce) {
    var sum = 0;
    for(var i = 0; i < values.length; i++) {
      sum = sum + values[i];
    }
    return(sum);

  },
  '_stats': function(key, values, rereduce) {
    return null;
  }
};

function count(values) {
  return BUILTIN_REDUCERS._count(undefined, values, false);
}
function sum(values) {
  return BUILTIN_REDUCERS._sum(undefined, values, false);
}