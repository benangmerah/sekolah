var fs = require('fs');
var util = require('util');
var url = require('url');
var log = require('winston');
var async = require('async');
var cheerio = require('cheerio');
var request = require('request');
var minimist = require('minimist');
var _s = require('underscore.string');
var n3 = require('n3');

var endpoint = 'http://referensi.data.kemdikbud.go.id/index11.php';
var schools = [];

var outputTurtle = 'schools.ttl';

var rdfNS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var rdfsNS = 'http://www.w3.org/2000/01/rdf-schema#';
var owlNS = 'http://www.w3.org/2002/07/owl#';
var geoNS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
var iadoNS = 'http://sw.benangmerah.net/ontology/idn-adm-div/';
var placeNS = 'http://sw.benangmerah.net/place/idn/';
var bmNS = 'http://sw.benangmerah.net/ontology#';
var npsnNS = 'urn:npsn:';
var altNpsnNS = 'http://referensi.data.kemdikbud.go.id/tabs.php?npsn=';
var dapodikNS = 'http://referensi.data.kemdikbud.go.id/#';
var prefixes = {
  rdf: rdfNS,
  rdfs: rdfsNS,
  owl: owlNS,
  npsn: npsnNS,
  wgs84_pos: geoNS,
  bm: bmNS,
  '': dapodikNS
};

var writeStreamTurtle = fs.createWriteStream(outputTurtle);
var triples = new n3.Writer(writeStreamTurtle, prefixes);

var argv = minimist(process.argv.slice(2));
if (argv.threshold) {
  var threshold = argv.threshold;
}
else if (argv.t) {
  var threshold = argv.t;
}
else {
  var threshold = 1;
}

// province is in the form { url: [url], name: [name] }

var pageQueue = async.queue(function processPage(page, callback) {
  // page is an object referring to a page on the endpoint
  log.info('Processing page: ' + page.name + ' (' + page.url + ')');

  var absoluteURL = url.resolve(endpoint, page.url);

  request(absoluteURL, function processPageResponse(err, results, body) {
    if (err) {
      log.error(err);
    }
    else {
      log.info('Successfully loaded ' + page.name + ' (' + page.url + ')');
      var $ = cheerio.load(body);

      if (page.level == 'school') {
        processSchool($, page, callback);
      }
      else {
        processRegion($, page, callback);
      }
    }
  })
}, threshold);

function processSchool($, page, callback) {
  log.info('Processing school NPSN ' + page.name);

  var npsn = page.name;
  var schoolData = {};


  var tables = $('#tabs table');
  tables.each(function processTable(index, tableElement) {
    var rows = $('tr', $(tableElement));
    rows.each(function(index, trElement) {
      var cells = $('td', $(trElement));
      if (cells.length > 0) {
        var fieldName = $(cells[1]).text().trim();
        var fieldValue = $(cells[3]).text().trim();

        if (fieldValue === '-') {
          fieldValue = '';
        }

        // JSON processing
        if (fieldName) {
          schoolData[fieldName] = fieldValue;
        }
      }
    })
  });

  var mapIframe = $('#tabs-8 iframe');
  var mapIframeSrc = mapIframe.attr('src');
  var mapIframeUrlFragments = url.parse(mapIframeSrc, true);
  var coordinates = mapIframeUrlFragments.query;
  schoolData.latitude = coordinates.x;
  schoolData.longitude = coordinates.y;

  // RDF processing
  processSchoolRDF(schoolData);
  schools.push(schoolData);

  callback();
}

function processRegion($, page, callback) {
  log.info('Processing ' + page.level + ': ' + page.name);

  var currentLevel = page.level;
  if (!currentLevel) {
    var nextLevel = 'province';
  }
  else if (currentLevel == 'province') {
    var nextLevel = 'regency';
  }
  else if (currentLevel == 'regency') {
    var nextLevel = 'district';
  }
  else if (currentLevel == 'district') {
    var nextLevel = 'school';
  }

  if (currentLevel == 'district') {
    var links = $('#example td a');
  }
  else {
    var links = $('#box-table-a tbody a');
  }

  links.each(function(i, element) {
    var a = $(element);
    var url = a.attr('href');
    var name = a.text().trim();

    log.info('Adding page ' + name + ' to queue...');
    pageQueue.push({
      level: nextLevel,
      url: url,
      name: name,
      parent: page
    });
  });

  callback();
}

function processSchoolRDF(schoolData) {
  var npsn = schoolData.NPSN;

  triples.addTriple(npsnNS + npsn, owlNS + 'sameAs', altNpsnNS + npsn);
  triples.addTriple(npsnNS + npsn, rdfsNS + 'seeAlso', altNpsnNS + npsn);

  for (var fieldName in schoolData) {
    var fieldValue = schoolData[fieldName];
    if (!fieldValue || fieldValue === '0'
      || fieldName == 'latitude' || fieldName == 'longitude')
      continue;

    var cleanFieldName = fieldName.replace(/[\.\/\(\)]/g, '');
    var subjectURI = npsnNS + npsn;
    var propertyURI = dapodikNS + _s.camelize(cleanFieldName);

    if (cleanFieldName == 'Waktu Penyelenggaraan'
      || cleanFieldName == 'Jenjang Pendidikan'
      || cleanFieldName == 'Status Sekolah') {
      var fieldValueRDF = dapodikNS + fieldValue;
    }
    else if (cleanFieldName == 'Email') {
      var fieldValueRDF = 'mailto:' + fieldValue;
    }
    else if (cleanFieldName == 'Website') {
      var fieldValueRDF = 'http://' + fieldValue;
    }
    else {
      fieldValueRDF = '"' + fieldValue + '"';
    }
    triples.addTriple(subjectURI, propertyURI, fieldValueRDF);
  }

  triples.addTriple(npsnNS + npsn, geoNS + 'latitude', '"' + schoolData.latitude + '"');
  triples.addTriple(npsnNS + npsn, geoNS + 'longitude', '"' + schoolData.longitude + '"');
  triples.addTriple(npsnNS + npsn,
    bmNS + 'isInsideAdministrativeDivision',
    canonicalPlaceURI(schoolData));
}

function canonicalPlaceURI(schoolData) {
  var province = schoolData['Propinsi'];
  var regency = schoolData['Kabupaten/Kota'];
  var district = schoolData['Kecamatan'];

  var isDKI = /Jakarta$/i.test(province);
  if (isDKI) {
    province = 'DKI JAKARTA';
  }

  var isDIY = /Yogyakarta$/i.test(province);
  if (isDIY) {
    province = 'DAERAH ISTIMEWA YOGYAKARTA';
  }

  var isKabupaten = /^Kab\. /i.test(regency);
  if (isKabupaten) {
    if (isDKI) {
      regency = 'Kabupaten Administrasi ' + regency.substring(4);
    }
    else {
      regency = regency.substring(4);
    }
  }
  else if (isDKI) {
    regency = 'Kota Administrasi' + regency.substring(5);
  }

  district = district.substring(4);

  var canonicalProvince = _s.slugify(province);
  var canonicalRegency = _s.slugify(regency);
  var canonicalDistrict = _s.slugify(district);

  var placeURI = placeNS
    + canonicalProvince + '/'
    + canonicalRegency + '/'
    + canonicalDistrict;

  return placeURI;
}

pageQueue.drain = function () {
  fs.writeFileSync('schools.json', JSON.stringify(schools, null, '  '));
  triples.end();
}

// Begin the queue and let the magic begin
pageQueue.push({
  url: endpoint,
  name: 'INDONESIA'
});