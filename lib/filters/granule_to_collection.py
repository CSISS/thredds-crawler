import os
import string
import re
import io
from lxml import etree as ETree


from .xml_editor import XMLEditor
from ..util.dtutil import timestamp_re, timestamp_range_generator
from ..util.path import slugify

iso_template_path = os.path.join(os.path.dirname(__file__), "../../templates/unidata-thredds-collection.xml.template")


class GranuleToCollection():
    def __init__(self, output_dir='../records/collections'):
        template_string = open(iso_template_path, "r").read()
        self.template = string.Template(template_string)
        self.output_dir = output_dir
        # print("Using template '%s' to generate ISO metadata" % iso_template_path)


    def generate_template_keywords(self, ds, xml_doc):
        file_identifier = ds.collection_name
        title = file_identifier.split('/')[-1]

        keywords = {}
        keywords["file_identifier"] = file_identifier
        keywords["title"] = title
        keywords["responsible_party"] = xml_doc.get_xpath_text('/gmi:MI_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString')
        keywords["contact_email"] = xml_doc.get_xpath_text('/gmi:MI_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString')
        keywords["date"] = xml_doc.get_xpath_text('/gmi:MI_Metadata/gmd:dateStamp/gco:Date')
        keywords["live_catalog_url"] = ds.collection_catalog_url
        keywords["source_granule_url"] = ds.iso_md_url.replace('&', '&amp;')
        return(keywords)


    def copy_elements_to_collection_xml(self, collection_xml, dataset_xml):
        e = dataset_xml.get_xpath_element('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords')
        collection_xml.append_element_to_xpath('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification', e)

        copy_xpaths = [('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords',
        '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification'),
        
        ('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory',
        '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification'),
        
        ('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent',
        '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification'),
        
        ('/gmi:MI_Metadata/gmd:contentInfo',
        '/gmi:MI_Metadata'),
        
        ('/gmi:MI_Metadata/gmd:dataQualityInfo',
        '/gmi:MI_Metadata'),
        
        ('/gmi:MI_Metadata/gmd:metadataMaintenance',
        '/gmi:MI_Metadata')]

        for src_xpath, dest_xpath in copy_xpaths:
            e = dataset_xml.get_xpath_element(src_xpath)
            collection_xml.append_element_to_xpath(dest_xpath, e)


    def set_collection_temporal_extent(self, xml_doc):
        # TODO: extract timestamp from collection siphon object
        # HACK: assume 14 days duration
        ts_range = timestamp_range_generator(14)
        
        # xml_str = """<gml:description>seconds</gml:description>
        #         <gml:beginPosition>%s</gml:beginPosition>
        #         <gml:endPosition>%s</gml:endPosition>""" % (ts_range.start_timestamp,  ts_range.end_timestamp)

        xml_doc.update_xpath_text('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition', ts_range.start_timestamp)
        xml_doc.update_xpath_text('/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition', ts_range.end_timestamp)

                  # <gmd:EX_TemporalExtent id="boundingTemporalExtent">
                  #    <gmd:extent>
                  #       <gml:TimePeriod gml:id="d3009">
                  #          <gml:description>seconds</gml:description>
                  #          <gml:beginPosition>2018-09-25T03:00:00Z</gml:beginPosition>
                  #          <gml:endPosition>2018-09-28T12:00:00Z</gml:endPosition>
                  #       </gml:TimePeriod>

    def generate_collection_iso_for_dataset(self, dataset, ds_file):
        dataset_xml_doc = XMLEditor.fromfile(ds_file)

        template_keywords = self.generate_template_keywords(dataset, dataset_xml_doc)

        collection_xml_text = self.template.substitute(template_keywords)
        collection_xml_doc = XMLEditor.fromstring(collection_xml_text)

        self.copy_elements_to_collection_xml(collection_xml_doc, dataset_xml_doc)

        self.set_collection_temporal_extent(collection_xml_doc)

        collection_file = self.output_dir + '/' + slugify(template_keywords['title']) + '.xml'

        collection_xml_doc.tofile(collection_file)
        print("[GENERATED]     %s" % collection_file)
