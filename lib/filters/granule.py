import string
import os

from .xml_editor import XMLEditor

class CommonFilter():
    def is_required(ds, text):
        return True

    def apply(ds, text):
        xml = XMLEditor.fromstring(text)

        file_identifier = "<gco:CharacterString xmlns:gco=\"http://www.isotc211.org/2005/gco\">%s</gco:CharacterString>" % ds.authority_ns_id
        xml.update_xpath_fromstring('/gmi:MI_Metadata/gmd:fileIdentifier', file_identifier)

        result = xml.tostring()
        
        return(result)

rda_iso_http_template_path = os.path.join(os.path.dirname(__file__), "../../templates/rda-thredds-http-service-identification.xml.template")
rda_iso_http_template = string.Template(open(rda_iso_http_template_path, "r").read())

class RDAFilter():
    def is_required(ds, text):
        return ds.authority == 'edu.ucar.rda'

    def apply(ds, text):
        url = 'https://rda.ucar.edu/' + ds.id.replace('files/g', 'data')

        http_service_id_text = rda_iso_http_template.substitute({'url': url, 'title': ds.name})

        http_service_id_elem = XMLEditor.fromstring(http_service_id_text).get_xpath_element('/gmd:identificationInfo')
        print(http_service_id_elem)
        
        xml = XMLEditor.fromstring(text)
        xml.append_element_to_xpath('/gmi:MI_Metadata', http_service_id_elem)


        return(xml.tostring())
