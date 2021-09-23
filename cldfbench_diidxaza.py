from collections import ChainMap, defaultdict, OrderedDict
import contextlib
import pathlib
import sqlite3

from cldfbench import CLDFSpec, Dataset as BaseDataset
import rfc3986


ENTRY_MAP = {
    'EntryID': 'ID',
    'POS': 'Part_Of_Speech',
}

SENSE_MAP = {
    'SenseID': 'ID',
    'EntryID': 'Entry_ID',
    'EngDesc': 'Description',
    'SpaDesc': 'alt_translation1',
    'ZapDesc': 'alt_translation2',
    'SpecimenImagesByTaxa': 'Media_IDs',
}

EXAMPLE_MAP = {
    'ExampleID': 'ID',
    'Senses': 'Sense_IDs',
    'ZapPDLMAText': 'alt_translation1',
    'ZapAPText': 'Primary_Text',
    'SpaText': 'alt_translation2',
    'EngText': 'Translated_Text',
    'Audio': 'Media_IDs',
}


def split_arrays(row, array_cols):
    return {
        k: [i.strip() for i in v.split(array_cols[k])] if k in array_cols else v
        for k, v in row.items()}


def add_media_metadata(media_catalog, media_row):
    if media_row.get('ID') in media_catalog:
        metadata = {
            'URL': rfc3986.uri.URIReference.from_string(
                'https://cdstar.eva.mpg.de/bitstreams/{0[objid]}/{0[original]}'.format(
                    media_catalog[media_row['ID']])),
            'mimetype': media_catalog[media_row['ID']]['mimetype'],
            'size': media_catalog[media_row['ID']]['size'],
        }
        return ChainMap(media_row, metadata)
    else:
        return media_row


def authors_string(authors):
    """Return formatted string of all authors."""
    def is_primary(a):
        return not isinstance(a, dict) or a.get('primary', True)

    primary = ' and '.join(
        a['name'] if isinstance(a, dict) else a
        for a in authors
        if is_primary(a))
    secondary = ' and '.join(
        a['name']
        for a in authors
        if not is_primary(a))
    if primary and secondary:
        return '{} with {}'.format(primary, secondary)
    else:
        return primary or secondary


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "diidxaza"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module='Dictionary',
            metadata_fname='cldf-metadata.json')

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        pass

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """

        # read data

        md = self.etc_dir.read_json('md.json')
        properties = md.get('properties') or {}
        language_name = md['language']['name']
        isocode = md['language']['isocode']
        language_id = md['language']['isocode']
        glottocode = md['language']['glottocode']

        if (self.etc_dir / 'cdstar.json').exists():
            media_catalog = self.etc_dir.read_json('cdstar.json')
        else:
            media_catalog = {}

        with contextlib.closing(sqlite3.connect(self.raw_dir / 'plantsdb-dictionaria-20190904.sqlite')) as conn:
            cu = conn.cursor()
            cu.execute('SELECT * FROM entries;');
            keys = [x[0] for x in cu.description]
            entries = [
                OrderedDict(zip(keys, row))
                for row in cu.fetchall()]
            cu.execute('SELECT * FROM senses;');
            keys = [x[0] for x in cu.description]
            senses = [
                OrderedDict(zip(keys, row))
                for row in cu.fetchall()]
            cu.execute('SELECT * FROM examples WHERE zapaptext IS NOT NULL;');
            keys = [x[0] for x in cu.description]
            examples = [
                OrderedDict(zip(keys, row))
                for row in cu.fetchall()]

        # processing

        entries = [
            OrderedDict((k, v.strip()) for k, v in row.items() if v and v.strip())
            for row in entries]
        senses = [
            OrderedDict((k, v.strip()) for k, v in row.items() if v and v.strip())
            for row in senses]
        examples = [
            OrderedDict((k, v.strip()) for k, v in row.items() if v and v.strip())
            for row in examples]

        media_dict = defaultdict(set)
        for sense in senses:
            if 'AssociatedTaxa' in sense:
                taxa = sense['AssociatedTaxa'].split(';')
                images = sense.get('SpecimenImagesByTaxa', '').split(';')
                assert len(taxa) == len(images)
                sense['SpecimenImagesByTaxa'] = ','.join(img for img in images if img)

                for taxon, imgs in zip(taxa, images):
                    for img in imgs.split(','):
                        if img:
                            media_dict[img].add(taxon)

        for ex in examples:
            if 'Audio' in ex:
                media_dict[ex['Audio']] = ex.get('ZipAPText', '')

        entries = [
            {ENTRY_MAP.get(k, k): v for k, v in row.items()}
            for row in entries]
        entries = [split_arrays(row, {'Sources': ';'}) for row in entries]
        for entry in entries:
            entry['Language_ID'] = language_id

        senses = [
            {SENSE_MAP.get(k, k): v for k, v in row.items()}
            for row in senses]
        senses = [split_arrays(row, {'Media_IDs': ','}) for row in senses]

        examples = [
            {EXAMPLE_MAP.get(k, k): v for k, v in row.items()}
            for row in examples]
        examples = [
            split_arrays(row, {'Media_IDs': ';', 'Sources': ';', 'Sense_IDs': ';'})
            for row in examples]
        for example in examples:
            example['Language_ID'] = language_id

        media = [
            {
                'ID': k,
                'Description': 'Taxa: {}'.format(', '.join(sorted(v))) if isinstance(v, set) else v,
            }
            for k, v in sorted(media_dict.items())]
        media = [add_media_metadata(media_catalog, row) for row in media]

        # cldf schema

        args.writer.cldf.add_component(
            'ExampleTable',
            {
                'name': 'alt_translation1',
                'datatype': 'string',
                'titles': properties['metalanguages']['gxx'],
            },
            {
                'name': 'alt_translation2',
                'datatype': 'string',
                'titles': properties['metalanguages']['gxy'],
            },
            {
                'name': 'Sense_IDs',
                'datatype': 'string',
                'separator': ' ; ',
            },
            {
                'name': 'Media_IDs',
                'datatype': 'string',
                'separator': ' ; ',
                'titles': 'Audio',
            },
            {
                'name': 'Sources',
                'datatype': {'base': 'string'},
                'titles': 'Speaker',
                'separator': ';',
            })

        args.writer.cldf.add_table(
            'media.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'http://cldf.clld.org/v1.0/terms.rdf#languageReference',
            'http://cldf.clld.org/v1.0/terms.rdf#description',
            'Filename',
            {'name': 'URL', 'datatype': 'anyURI'},
            'mimetype',
            {'name': 'size', 'datatype': 'integer'})

        args.writer.cldf.add_component('LanguageTable')

        args.writer.cldf.add_columns(
            'EntryTable',
            {
                'name': 'HeadwordPDLMA',
                'datatype': 'string',
                'titles': 'PDLMA',
            },
            {
                'name': 'Sources',
                'datatype': {'base': 'string'},
                'titles': 'Speaker',
                'separator': ';',
            })

        args.writer.cldf.add_columns(
            'SenseTable',
            {
                'name': 'alt_translation1',
                'datatype': 'string',
                'titles': properties['metalanguages']['gxx'],
            },
            {
                'name': 'alt_translation2',
                'datatype': 'string',
                'titles': properties['metalanguages']['gxy'],
            },
            {
                'name': 'Media_IDs',
                'datatype': 'string',
                'titles': 'SpecimenImagesByTaxa',
                'separator': ',',
            },
            {
                'name': 'AssociatedTaxa',
                'datatype': 'string',
                'titles': 'Taxa',
            })

        args.writer.cldf.add_foreign_key(
            'SenseTable', 'Media_IDs', 'media.csv', 'ID')
        args.writer.cldf.add_foreign_key(
            'ExampleTable', 'Media_IDs', 'media.csv', 'ID')
        args.writer.cldf.add_foreign_key(
            'ExampleTable', 'Sense_IDs', 'SenseTable', 'ID')


        # output

        args.writer.cldf.properties['dc:creator'] = authors_string(
            md.get('authors') or ())

        language = {
            'ID': language_id,
            'Name': language_name,
            'ISO639P3code': isocode,
            'Glottocode': glottocode,
        }
        args.writer.objects['LanguageTable'] = [language]

        args.writer.objects['EntryTable'] = entries
        args.writer.objects['SenseTable'] = senses
        args.writer.objects['ExampleTable'] = examples
        args.writer.objects['media.csv'] = media
