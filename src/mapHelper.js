// TODO: add short names
const short = {
  water: 'wt',
  road: 'rd',
  fillColor: 'fc',
  labelColor: 'lbc',
  landColor: 'lc'
}

function constructKeyValuePairs(dict) {
  let s = '';
  for (let [key, value] of Object.entries(dict)) {
    s += short[key] + ':' + value.slice(1) + ';';
  }

  // remove trailing semicolon
  if (s) s = s.slice(0, -1)

  return s;
}

export function customMapStyleToQueryParam(customStyles) {
  if (!customStyles.elements && !customStyles.settings) return null;

  let s = '';

  if (customStyles.elements) {
    for (let [key, value] of Object.entries(customStyles.elements)) {
      s += '_' + short[key] + '|' + constructKeyValuePairs(value);
    }
  }

  if (customStyles.settings) {
    s += '_g|' + constructKeyValuePairs(customStyles.settings);
  }

  // remove first underscore
  if (s) s = s.slice(1);

  return 'st=' + s;
}