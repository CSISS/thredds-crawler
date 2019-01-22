class CommonFilter():
    def is_required(ds, text):
        return True

    def apply(ds, text):
        xml = XMLEditor.fromstring(text)

        file_identifier = "<gco:CharacterString xmlns:gco=\"http://www.isotc211.org/2005/gco\">%s</gco:CharacterString>" % ds.authority_ns_id
        xml.update_xpath_fromstring('/gmi:MI_Metadata/gmd:fileIdentifier', file_identifier)

        return(xml.string(xml))


class RDAFilter():
    def is_required(ds, text):
        return True

    def apply(ds, text):
        return(text + "THIS IS RDA")
        # xml = XMLEditor.fromstring(text)

        # file_identifier = "<gco:CharacterString xmlns:gco=\"http://www.isotc211.org/2005/gco\">%s</gco:CharacterString>" % ds.authority_ns_id
        # xml.update_xpath_fromstring('/gmi:MI_Metadata/gmd:fileIdentifier', file_identifier)

        # return(xml.string(xml))