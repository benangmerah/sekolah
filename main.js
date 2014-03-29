var fs = require('fs');
var util = require('util');
var url = require('url');
var logger = require('winston');
var async = require('async');
var cheerio = require('cheerio');
var request = require('request');
var minimist = require('minimist');
var _s = require('underscore.string');
var n3 = require('n3');
var csvWriter = require('csv-write-stream')({ newline: '\r\n' });

var endpoint = 'http://referensi.data.kemdikbud.go.id/index11.php';
// var schools = [];

var maxRetries = 10;

var outputTurtle = 'schools.ttl';
var outputCSV = 'schools.csv';

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

var writeStreamCSV = fs.createWriteStream(outputCSV);
csvWriter.pipe(writeStreamCSV);

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

var timeout = argv.timeout || 5000;

logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {
  level: argv.v ? 'verbose' : 'info',
  colorize: true,
  timestamp: true
});
if (argv.log) {
  logger.add(logger.transports.File, {
    level: 'verbose',
    filename: argv.log
  });
}

// province is in the form { url: [url], name: [name] }

var pageQueue = async.queue(function processPage(page, callback) {
  // page is an object referring to a page on the endpoint
  logger.info('Loading page: ' + page.name + ' (' + page.url + ')...');

  var absoluteURL = url.resolve(endpoint, page.url);

  request({
    url: absoluteURL,
    timeout: timeout,
    headers: {
      'User-Agent': 'request/2.34.1 (Crawler untuk Tugas Akhir. Mohon maaf apabila mengganggu.)'
    }
  }, function processPageResponse(err, results, body) {
    if (err) {
      logger.error('Failed loading ' + page.name + ' (' + page.url + '): ' + err);

      if (!page.retries || page.retries <= maxRetries) {
        page.retries = page.retries ? page.retries + 1 : 1;
        logger.warn('Re-entering ' + page.name + ' (' + page.url + ') to queue. (Retry #' + page.retries + ')');
        pageQueue.push(page);
        callback();
      }
      else {
        logger.error('Maximum number of retries reached for ' + page.name + ' (' + page.url + ').');
      }
    }
    else {
      logger.info('Successfully loaded page: ' + page.name + ' (' + page.url + ').');
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
  logger.info('Processing school NPSN=' + page.name + '...');

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
  csvWriter.write(schoolData);
  // schools.push(schoolData);


  logger.info('Finished processing school NPSN=' + page.name + '.');

  callback();
}

function processRegion($, page, callback) {
  logger.info('Processing ' + page.level + ': ' + page.name);

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

  var subQueue = [];
  links.each(function(i, element) {
    var a = $(element);
    var url = a.attr('href');
    var name = a.text().trim();

    subQueue.push({
      level: nextLevel,
      url: url,
      name: name,
      parent: page
    });
    logger.verbose('Added page ' + name + ' to queue.');
  });

  pageQueue.push(subQueue);
  logger.info('Added ' + subQueue.length + ' pages to queue.');

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
      regency = 'Kabupaten ' + regency.substring(4).trim();
    }
  }
  else if (isDKI) {
    regency = 'Kota Administrasi ' + regency.substring(5).trim();
  }

  district = district.substring(4).trim();

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
  // fs.writeFileSync('schools.json', JSON.stringify(schools, null, '  '));
  triples.end();
}

// Begin the queue and let the magic begin
logger.info('Welcome to Dapodik crawler!');
logger.info('Request timeout is set to ' + timeout + 'ms');
logger.info('Outputting Turtle RDF data to ' + outputTurtle);
logger.info('Outputting CSV data to ' + outputCSV);
if (argv.log)
  logger.info('Outputting verbose log data to ' + argv.log);

pageQueue.push({
  url: endpoint,
  // url: 'http://referensi.data.kemdikbud.go.id/index11.php?kode=060101&level=3',
  // level: 'district',
  name: 'INDONESIA'
});