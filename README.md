# Indonesian Schools as Linked Data

The Indonesian Ministry of Education and Culture (Kemdikbud) has compiled an extensive databases of (primary and secondary) schools in Indonesia. They are browsable in HTML format on the [Data Referensi Pendidikan](http://referensi.data.kemdikbud.go.id/) website.

This repository contains a node.js script to scrape the school data from the website and generate:
* An RDF/Turtle graph file for Linked Data applications.
* A CSV file covering all schools, included as a convenience.

Kemdikbud identifies each school through a unique NPSN (_Nomor Pokok Sekolah Nasional_), and the permalink for a profile of a school at the website is at `http://referensi.data.kemdikbud.go.id/tabs.php?npsn={npsn}`. This permalink URI is the one used for the subject part in the RDF triples.

The RDF predicates are properties of the school available on the school's profile page. The RDF objects are the values.